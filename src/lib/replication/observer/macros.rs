macro_rules! process_event(
    ($observer:expr, $schema:expr, [$($event:expr),+]) => {
        let observer = $observer;
        let schema = $schema;
        $(observer.process_event(&$event, &schema).await?;)+

        drop(observer);
    }
);
