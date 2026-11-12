package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"

	"starrocks-api/internal/config"
	"starrocks-api/internal/handler"
	"starrocks-api/internal/middleware"
	"starrocks-api/internal/pool"
)

func main() {
	configPath := flag.String("config", "config.json", "path to config file")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}
	slog.Info("config loaded", "environments", len(cfg.Environments))

	// Create connection pool manager
	pm, err := pool.NewManager(cfg.Environments)
	if err != nil {
		slog.Error("failed to create pool manager", "err", err)
		os.Exit(1)
	}
	defer pm.Close()

	// Create handlers
	docsH := handler.NewDocsHandler()
	authH := handler.NewAuthHandler(cfg)
	healthH := handler.NewHealthHandler(pm)
	envsH := handler.NewEnvironmentsHandler(pm)
	queryH := handler.NewQueryHandler(pm)
	exportH := handler.NewExportHandler(pm)
	dashH := handler.NewDashboardHandler(pm)
	schemaH := handler.NewSchemaHandler(pm)
	rlH := handler.NewRoutineLoadHandler(pm)
	queriesH := handler.NewQueriesHandler(pm)
	batchH := handler.NewBatchLoadHandler(pm)
	schedH := handler.NewSchedulerHandler(pm)
	pipeH := handler.NewPipeHandler(pm)
	mvH := handler.NewMaterializedViewHandler(pm)
	partH := handler.NewPartitionHandler(pm)

	// Build router
	r := chi.NewRouter()

	// Global middleware
	r.Use(middleware.Logging)
	r.Use(middleware.CORS(cfg.Server.CORSOrigins))

	// Public routes (no auth required)
	// Probe paths follow the deployment chart's defaults (/health/*); the
	// /api/* pair is kept as an alias for existing callers.
	r.Get("/health/live", healthH.Live)
	r.Get("/health/ready", healthH.Ready)
	r.Get("/health/startup", healthH.Startup)
	r.Get("/api/health", healthH.Live)
	r.Get("/api/ready", healthH.Ready)
	r.Post("/api/login", authH.Login)
	r.Get("/api/docs", docsH.UI)
	r.Get("/api/docs/openapi.yaml", docsH.Spec)

	// Auth-protected routes
	r.Group(func(r chi.Router) {
		r.Use(middleware.RequireAuth(cfg))

		r.Get("/api/environments", envsH.List)

		// Environment-scoped routes (read-accessible by all roles)
		r.Group(func(r chi.Router) {
			r.Use(middleware.RequireEnv(pm))

			// Read-only endpoints (viewer + admin)
			r.Get("/api/nodes", dashH.Nodes)
			r.Get("/api/ingestion", dashH.Ingestion)
			r.Get("/api/routines", dashH.Routines)
			r.Get("/api/databases", schemaH.Databases)
			r.Get("/api/tables", schemaH.Tables)
			r.Get("/api/columns", schemaH.Columns)
			r.Get("/api/table-details", schemaH.TableDetails)
			r.Get("/api/show-create", schemaH.ShowCreateDDL)
			r.Post("/api/query", queryH.Execute)
			r.Post("/api/export", exportH.Export)
			r.Get("/api/routine-load/list", rlH.ListForDB)

			// Query monitoring (viewer + admin)
			r.Get("/api/process-list", queriesH.ProcessList)
			r.Get("/api/current-queries", queriesH.CurrentQueries)
			r.Get("/api/slow-queries", queriesH.SlowQueries)

			// Batch load (viewer can list)
			r.Get("/api/batch-load/list", batchH.List)

			// Scheduled tasks (viewer can list + view run history)
			r.Get("/api/scheduled-tasks", schedH.List)
			r.Get("/api/scheduled-tasks/runs", schedH.Runs)

			// Pipes — continuous ingestion (viewer can list + view files)
			r.Get("/api/pipes", pipeH.List)
			r.Get("/api/pipes/files", pipeH.Files)

			// Materialized views + partitions (viewer can list)
			r.Get("/api/materialized-views", mvH.List)
			r.Get("/api/partitions", partH.List)

			// Admin-only write endpoints
			r.Group(func(r chi.Router) {
				r.Use(middleware.RequireAdmin())

				r.Post("/api/add-column", schemaH.AddColumn)
				r.Post("/api/create-table", schemaH.CreateTable)
				r.Post("/api/admin/exec", schemaH.AdminExec)
				r.Post("/api/routine-load/execute", rlH.Execute)
				r.Post("/api/routine-load/alter", rlH.Alter)
				r.Post("/api/kill-query", queriesH.KillQuery)
				r.Post("/api/batch-load/execute", batchH.Execute)
				r.Post("/api/batch-load/cancel", batchH.Cancel)
				r.Post("/api/stream-load", batchH.StreamLoad)

				r.Post("/api/scheduled-tasks", schedH.Create)
				r.Post("/api/scheduled-tasks/update", schedH.Update)
				r.Post("/api/scheduled-tasks/trigger", schedH.Trigger)
				r.Post("/api/scheduled-tasks/drop", schedH.Drop)

				r.Post("/api/pipes", pipeH.Create)
				r.Post("/api/pipes/alter", pipeH.Alter)
				r.Post("/api/pipes/set", pipeH.SetProperty)
				r.Post("/api/pipes/retry", pipeH.Retry)
				r.Post("/api/pipes/drop", pipeH.Drop)

				r.Post("/api/materialized-views", mvH.Create)
				r.Post("/api/materialized-views/refresh", mvH.Refresh)
				r.Post("/api/materialized-views/drop", mvH.Drop)
				r.Post("/api/partitions/exec", partH.Exec)
				r.Post("/api/export-files", batchH.ExportFiles)
			})
		})
	})

	// Create HTTP server
	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeoutSeconds) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeoutSeconds) * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("server starting", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "err", err)
	}

	slog.Info("server stopped")
}
