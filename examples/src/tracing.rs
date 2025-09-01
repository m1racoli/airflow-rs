use airflow_common::datetime::StdTimeProvider;
use airflow_common::datetime::TimeProvider;
use airflow_common::datetime::UtcDateTime;
use airflow_common::models::TaskInstanceKey;
use airflow_common::utils::MapIndex;
use std::fmt;
use tokio::sync::mpsc::UnboundedSender;
use tracing::Collect;
use tracing::Level;
use tracing::Metadata;
use tracing::field::Field;
use tracing::field::Visit;
use tracing::span::Attributes;
use tracing::span::Id;
use tracing_subscriber::Subscribe;
use tracing_subscriber::field::VisitFmt;
use tracing_subscriber::field::VisitOutput;
use tracing_subscriber::fmt::format::DefaultVisitor;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::registry::SpanRef;
use tracing_subscriber::subscribe::Context;
use tracing_subscriber::subscribe::Filter;

#[derive(Debug, Default)]
pub struct TaskInstanceKeyLayer;

impl<C: Collect + for<'a> LookupSpan<'a>> Subscribe<C> for TaskInstanceKeyLayer {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, C>) {
        if let Some(span) = ctx.span(id) {
            let mut visitor = TaskInstanceKeyVisitor::default();
            attrs.record(&mut visitor);
            let key: Option<TaskInstanceKey> = visitor.into();
            match key {
                Some(k) => {
                    span.extensions_mut().insert(k);
                }
                None => {
                    if let Some(parent) = span.parent()
                        && let Some(k) = parent.extensions().get::<TaskInstanceKey>()
                    {
                        span.extensions_mut().insert(k.clone());
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum LogEvent {
    Message(TaskInstanceKey, UtcDateTime, String),
    Exit,
}

#[derive(Debug)]
pub struct TaskLogLayer {
    send: UnboundedSender<LogEvent>,
    time_provider: StdTimeProvider,
}

impl TaskLogLayer {
    pub fn new(send: UnboundedSender<LogEvent>) -> Self {
        let time_provider = StdTimeProvider;
        TaskLogLayer {
            send,
            time_provider,
        }
    }

    fn format_event<C: Collect + for<'a> LookupSpan<'a>>(
        &self,
        event: &tracing::Event<'_>,
        _ctx: &Context<'_, C>,
    ) -> Result<String, fmt::Error> {
        let meta = event.metadata();
        let mut buf = String::new();
        let mut writer = Writer::new(&mut buf);

        let timer = SystemTime;

        timer.format_time(&mut writer)?;
        writer.write_str(" {")?;

        // TODO improve handling of missing metadata
        if let Some(filename) = meta.module_path() {
            // tracing event
            write!(
                writer,
                "{}:{}",
                filename,
                if meta.line().is_some() { "" } else { " " }
            )?;

            if let Some(line) = meta.line() {
                write!(writer, "{}", line)?;
            }
        } else {
            // log event
            let mut log_visitor = LogVisitor::default();
            event.record(&mut log_visitor);

            let target = log_visitor.target.unwrap_or_else(|| "unknown".to_string());
            let line = match log_visitor.line {
                Some(line) => line.to_string(),
                None => "unknown".to_string(),
            };

            write!(writer, "{}:{}", target, line)?;
        }

        writer.write_str("} ")?;
        let fmt_level = FmtLevel::new(meta.level());
        write!(writer, "{}", fmt_level)?;

        writer.write_str(" -")?;

        let mut visitor = DefaultVisitor::new(writer, false);
        event.record(&mut visitor);
        let writer = visitor.writer();
        writer.write_char('\n')?;
        visitor.finish()?;
        Ok(buf)
    }
}

impl<C: Collect + for<'a> LookupSpan<'a>> Subscribe<C> for TaskLogLayer {
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, C>) {
        if let Some(span) = ctx.lookup_current()
            && let Some(key) = span.extensions().get::<TaskInstanceKey>()
        {
            match self.format_event(event, &ctx) {
                Ok(msg) => {
                    self.send
                        .send(LogEvent::Message(
                            key.clone(),
                            self.time_provider.now(),
                            msg,
                        ))
                        .ok();
                }
                Err(_) => {
                    eprintln!("Error formatting event");
                }
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct TaskContextFilter;

impl<C: Collect + for<'a> LookupSpan<'a>> Filter<C> for TaskContextFilter {
    fn enabled(&self, meta: &Metadata<'_>, ctx: &Context<'_, C>) -> bool {
        if meta.is_span() && meta_is_task(meta) {
            true
        } else {
            current_is_task(ctx)
        }
    }
}

#[derive(Debug, Default)]
pub struct NonTaskContextFilter;

impl<C: Collect + for<'a> LookupSpan<'a>> Filter<C> for NonTaskContextFilter {
    fn enabled(&self, meta: &Metadata<'_>, ctx: &Context<'_, C>) -> bool {
        if meta.is_span() {
            true
        } else {
            !current_is_task(ctx)
        }
    }
}

fn current_is_task<C: Collect + for<'a> LookupSpan<'a>>(ctx: &Context<'_, C>) -> bool {
    match ctx.lookup_current() {
        Some(span) => span_is_task(&span),
        None => false,
    }
}

fn span_is_task<C: Collect + for<'a> LookupSpan<'a>>(span: &SpanRef<'_, C>) -> bool {
    if meta_is_task(span.metadata()) {
        true
    } else {
        match span.parent() {
            Some(parent) => span_is_task(&parent),
            None => false,
        }
    }
}

fn meta_is_task(meta: &Metadata<'_>) -> bool {
    meta.target() == "task_context"
}

#[derive(Debug, Default)]
struct TaskInstanceKeyVisitor {
    dag_id: Option<String>,
    task_id: Option<String>,
    run_id: Option<String>,
    try_number: Option<usize>,
    map_index: Option<MapIndex>,
}

impl Visit for TaskInstanceKeyVisitor {
    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "try_number" {
            self.try_number = Some(value as usize);
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "map_index" {
            self.map_index = Some(value.try_into().unwrap()) // TODO
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "dag_id" {
            self.dag_id = Some(value.to_string());
        } else if field.name() == "task_id" {
            self.task_id = Some(value.to_string());
        } else if field.name() == "run_id" {
            self.run_id = Some(value.to_string());
        }
    }
}

impl From<TaskInstanceKeyVisitor> for Option<TaskInstanceKey> {
    fn from(visitor: TaskInstanceKeyVisitor) -> Self {
        match (
            visitor.dag_id,
            visitor.task_id,
            visitor.run_id,
            visitor.try_number,
            visitor.map_index,
        ) {
            (Some(dag_id), Some(task_id), Some(run_id), Some(try_number), Some(map_index)) => Some(
                TaskInstanceKey::new(&dag_id, &task_id, &run_id, try_number, map_index),
            ),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
struct LogVisitor {
    target: Option<String>,
    module_path: Option<String>,
    file: Option<String>,
    line: Option<u64>,
}

// extra fields for log events
impl Visit for LogVisitor {
    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "log.line" {
            self.line = Some(value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "log.target" {
            self.target = Some(value.to_string());
        } else if field.name() == "log.module_path" {
            self.module_path = Some(value.to_string());
        } else if field.name() == "log.file" {
            self.file = Some(value.to_string());
        }
    }
}

struct FmtLevel<'a> {
    level: &'a Level,
}

impl<'a> FmtLevel<'a> {
    pub(crate) fn new(level: &'a Level) -> Self {
        Self { level }
    }
}

const TRACE_STR: &str = "TRACE";
const DEBUG_STR: &str = "DEBUG";
const INFO_STR: &str = "INFO";
const WARN_STR: &str = "WARNING";
const ERROR_STR: &str = "ERROR";

impl<'a> fmt::Display for FmtLevel<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self.level {
            Level::TRACE => f.pad(TRACE_STR),
            Level::DEBUG => f.pad(DEBUG_STR),
            Level::INFO => f.pad(INFO_STR),
            Level::WARN => f.pad(WARN_STR),
            Level::ERROR => f.pad(ERROR_STR),
        }
    }
}
