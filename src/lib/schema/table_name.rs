use std::borrow::Cow;

pub(crate) fn table_name_without_prefix<T, P>(table_name: T, prefix: P) -> String
where
    T: AsRef<str>,
    P: AsRef<str>,
{
    let mut table_name = table_name.as_ref().to_owned();

    if prefix.as_ref().len() > 0 && table_name.starts_with(prefix.as_ref()) {
        table_name.drain(0..prefix.as_ref().len());
    }

    table_name
}

pub(crate) fn table_name_with_prefix<'a>(table_name: &'a str, prefix: &'a str) -> Cow<'a, str> {
    if prefix.len() > 0 {
        return Cow::Owned(format!("{prefix}{table_name}"));
    }
    Cow::Borrowed(table_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_table_name_intact_when_empty_prefix() {
        assert_eq!(table_name_without_prefix("table_name", ""), "table_name")
    }

    #[test]
    fn removes_prefix_from_table_name() {
        assert_eq!(
            table_name_without_prefix("my_custom_table", "my_"),
            "custom_table"
        )
    }

    #[test]
    fn keeps_original_table_name_if_name_does_not_have_it() {
        assert_eq!(
            table_name_without_prefix("custom_table", "my_"),
            "custom_table"
        )
    }

    #[test]
    fn keeps_table_name_the_same_when_prefix_is_empty() {
        assert_eq!(table_name_with_prefix("custom_table", ""), "custom_table")
    }

    #[test]
    fn adds_prefix_to_table_name_if_it_exists() {
        assert_eq!(
            table_name_with_prefix("custom_table", "my_"),
            "my_custom_table"
        );
    }
}
