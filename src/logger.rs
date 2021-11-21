use crate::types::Result;

struct NaiveLogger;

impl log::Log for NaiveLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() < log::STATIC_MAX_LEVEL
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            print!("[{}]", record.level());
            if let (Some(file), Some(line)) = (record.file(), record.line()) {
                print!(" {}:{}", file, line);
            }
            println!(" {}", record.args());
        }
    }

    fn flush(&self) {}
}

pub fn init() -> Result<()> {
    Ok(log::set_logger(&NaiveLogger).map(|()| log::set_max_level(log::LevelFilter::Info))?)
}
