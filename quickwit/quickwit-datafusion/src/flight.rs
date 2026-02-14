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
use tantivy_datafusion::{OpenerFactory, OpenerFactoryExt, TantivyCodec, full_text_udf};
use tonic::{Request, Response, Status, Streaming};

use crate::resolver::QuickwitWorkerResolver;

/// A Flight service that handles both:
/// - **df-distributed plan fragments** (worker execution)
/// - **SQL queries from external clients** (via `do_get` with SQL string tickets)
pub struct QuickwitFlightService {
    worker: Worker,
    opener_factory: OpenerFactory,
    searcher_pool: quickwit_search::SearcherPool,
}

impl QuickwitFlightService {
    fn build_client_session(&self) -> SessionContext {
        let mut config = SessionConfig::new().with_target_partitions(1);
        config.set_opener_factory(self.opener_factory.clone());

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

    async fn execute_sql(
        &self,
        sql: &str,
    ) -> Result<Response<BoxStream<'static, Result<FlightData, Status>>>, Status> {
        let ctx = self.build_client_session();

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
/// The `opener_factory` is registered on the worker's `SessionConfig`
/// for decoding plan fragments. It's also used by the client-facing
/// SQL execution path.
pub fn build_flight_service(
    opener_factory: OpenerFactory,
    searcher_pool: quickwit_search::SearcherPool,
) -> FlightServiceServer<QuickwitFlightService> {
    let factory_for_worker = opener_factory.clone();
    let worker = Worker::from_session_builder(
        move |ctx: datafusion_distributed::WorkerQueryContext| {
            let factory = factory_for_worker.clone();
            Box::pin(async move {
                let mut config = SessionConfig::new();
                config.set_opener_factory(factory);
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
        opener_factory,
        searcher_pool,
    })
}

// ── FlightService impl ──────────────────────────────────────────────

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
        if let Ok(sql) = std::str::from_utf8(&ticket.ticket) {
            if !sql.is_empty() && !sql.starts_with('\0') {
                return self.execute_sql(sql).await;
            }
        }
        self.worker.do_get(request).await
    }

    async fn handshake(&self, r: Request<Streaming<HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> { self.worker.handshake(r).await }
    async fn list_flights(&self, r: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> { self.worker.list_flights(r).await }
    async fn get_flight_info(&self, r: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> { self.worker.get_flight_info(r).await }
    async fn get_schema(&self, r: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> { self.worker.get_schema(r).await }
    async fn do_put(&self, r: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> { self.worker.do_put(r).await }
    async fn do_exchange(&self, r: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> { self.worker.do_exchange(r).await }
    async fn do_action(&self, r: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> { self.worker.do_action(r).await }
    async fn list_actions(&self, r: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> { self.worker.list_actions(r).await }
    async fn poll_flight_info(&self, r: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> { self.worker.poll_flight_info(r).await }
}
