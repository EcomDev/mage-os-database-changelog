macro_rules! process_event(
    ($observer:expr, $schema:expr, [$($event:expr),+]) => {
        let observer = $observer;
        $($crate::replication::EventObserver::process_event(&observer, &$event, &$schema).await?;)+

        drop(observer);
    }
);
