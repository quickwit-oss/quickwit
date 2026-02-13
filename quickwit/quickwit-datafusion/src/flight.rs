use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{DistributedExt, DistributedPhysicalOptimizerRule, Worker};
use futures::stream::BoxStream;
use futures::TryStreamExt;
use tantivy_datafusion::{
    IndexOpener, OpenerFactoryExt, OpenerMetadata, TantivyCodec, full_text_udf,
};
use tonic::{Request, Response, Status, Streaming};

use crate::resolver::QuickwitWorkerResolver;
use crate::split_opener::{SplitIndexOpener, SplitRegistry};

/// A Flight service that handles both:
/// - **df-distributed plan fragments** (worker execution)
/// - **SQL queries from external clients** (via `do_get` with SQL string tickets)
///
/// Dispatch: if the ticket decodes as a df-distributed protobuf, route
/// to the worker. Otherwise treat the ticket bytes as a UTF-8 SQL string.
pub struct QuickwitFlightService {
    worker: Worker,
    registry: Arc<SplitRegistry>,
    searcher_pool: quickwit_search::SearcherPool,
}

impl QuickwitFlightService {
    fn build_client_session(&self) -> SessionContext {
        let mut config = SessionConfig::new();
        let registry = self.registry.clone();
        config.set_opener_factory(Arc::new(move |meta: OpenerMetadata| {
            Arc::new(SplitIndexOpener::new(
                meta.identifier,
                registry.clone(),
                meta.tantivy_schema,
                meta.segment_sizes,
            )) as Arc<dyn IndexOpener>
        }));

        let worker_resolver = QuickwitWorkerResolver::new(self.searcher_pool.clone());

        let state = datafusion::execution::SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_distributed_worker_resolver(worker_resolver)
            .with_distributed_user_codec(TantivyCodec)
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .build();

        let ctx = SessionContext::new_with_state(state);
        ctx.register_udf(full_text_udf());
        ctx
    }

    /// Execute a SQL query and return a Flight stream of RecordBatches.
    async fn execute_sql(
        &self,
        sql: &str,
    ) -> Result<Response<BoxStream<'static, Result<FlightData, Status>>>, Status> {
        let ctx = self.build_client_session();

        // TODO: register index tables from metastore based on query.
        // For now, execute against an empty context (useful for SHOW TABLES, SELECT 1, etc.)

        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("SQL error: {e}")))?;
        let plan = df
            .create_physical_plan()
            .await
            .map_err(|e| Status::internal(format!("plan error: {e}")))?;
        let stream = execute_stream(plan.clone(), ctx.task_ctx())
            .map_err(|e| Status::internal(format!("execution error: {e}")))?;

        let schema = plan.schema();
        let flight_stream = arrow_flight::encode::FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream.map_err(|e| {
                arrow_flight::error::FlightError::ExternalError(Box::new(e))
            }))
            .map_err(|e| Status::internal(format!("flight encode error: {e}")));

        Ok(Response::new(Box::pin(flight_stream)))
    }
}

/// Build the combined Flight service.
///
/// Handles both df-distributed worker traffic AND external SQL queries
/// on the same gRPC port.
pub fn build_flight_service(
    registry: Arc<SplitRegistry>,
    searcher_pool: quickwit_search::SearcherPool,
) -> FlightServiceServer<QuickwitFlightService> {
    let reg = registry.clone();
    let worker = Worker::from_session_builder(
        move |ctx: datafusion_distributed::WorkerQueryContext| {
            let registry = reg.clone();
            Box::pin(async move {
                let mut config = SessionConfig::new();
                config.set_opener_factory(Arc::new(move |meta: OpenerMetadata| {
                    Arc::new(SplitIndexOpener::new(
                        meta.identifier,
                        registry.clone(),
                        meta.tantivy_schema,
                        meta.segment_sizes,
                    )) as Arc<dyn IndexOpener>
                }));

                Ok(ctx
                    .builder
                    .with_config(config)
                    .with_distributed_user_codec(TantivyCodec)
                    .build())
            })
        },
    );

    FlightServiceServer::new(QuickwitFlightService {
        worker,
        registry,
        searcher_pool,
    })
}

// ── FlightService impl: dispatch worker vs SQL ──────────────────────

#[tonic::async_trait]
impl FlightService for QuickwitFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.get_ref();

        // Try to parse as a df-distributed plan fragment.
        // df-distributed encodes DoGet as a prost::Message.
        // If the ticket is valid protobuf with the right fields, it's a worker request.
        // Otherwise, treat as UTF-8 SQL from an external client.
        if let Ok(sql) = std::str::from_utf8(&ticket.ticket) {
            // Heuristic: df-distributed tickets are protobuf (binary),
            // not valid UTF-8 text. If we got valid UTF-8 and it looks
            // like SQL (or any human-readable string), handle as SQL.
            // Pure protobuf tickets will almost never be valid UTF-8.
            if sql.len() > 0 && !sql.starts_with('\0') {
                return self.execute_sql(sql).await;
            }
        }

        // Delegate to df-distributed worker.
        self.worker.do_get(request).await
    }

    // All other methods delegate to the worker (which returns unimplemented for most).

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        self.worker.handshake(request).await
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.worker.list_flights(request).await
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.worker.get_flight_info(request).await
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.worker.get_schema(request).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        self.worker.do_put(request).await
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.worker.do_exchange(request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        self.worker.do_action(request).await
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        self.worker.list_actions(request).await
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        self.worker.poll_flight_info(request).await
    }
}
