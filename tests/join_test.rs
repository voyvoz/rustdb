#[cfg(test)]
mod tests {
    use rustdb::interface::*;
    use rustdb::dtype::*;

    fn create_test_relation(name: &str, columns: Vec<(&str, Vec<DataType>)>) -> ColumnStoreRelation {
        let mut relation = ColumnStoreRelation::new();
        relation.name = name.to_string();
        for (col_name, data) in columns {
            relation.columns.insert(col_name.to_string(), data);
            relation.fields.insert(col_name.to_string(), DataType::String(String::new()));
            relation.select_columns.push(col_name.to_string());
        }
        relation
    }

    macro_rules! assert_relation_eq {
        ($rel1:expr, $rel2:expr) => {
            assert_eq!($rel1.fields, $rel2.fields);
            assert_eq!($rel1.select_columns, $rel2.select_columns);
            assert_eq!($rel1.columns.len(), $rel2.columns.len());
            for (key, val) in &$rel1.columns {
                assert_eq!(val, $rel2.columns.get(key).unwrap());
            }
        };
    }

    #[test]
    fn test_nested_loop_join() {
        let relation1 = create_test_relation(
            "relation1",
            vec![
                ("id", vec![DataType::Int(1), DataType::Int(2), DataType::Int(3)]),
                ("value1", vec![DataType::String("A".to_string()), DataType::String("B".to_string()), DataType::String("C".to_string())]),
            ]
        );

        let relation2 = create_test_relation(
            "relation2",
            vec![
                ("id", vec![DataType::Int(2), DataType::Int(3), DataType::Int(4)]),
                ("value2", vec![DataType::String("X".to_string()), DataType::String("Y".to_string()), DataType::String("Z".to_string())]),
            ]
        );

        let result_relation = relation1.nested_loop_join(&relation2, "id", "id", |a, b| a == b).unwrap();

        let expected_relation = create_test_relation(
            "relation1_relation2_join",
            vec![
                ("id", vec![DataType::Int(2), DataType::Int(3)]),
                ("value1", vec![DataType::String("B".to_string()), DataType::String("C".to_string())]),
                ("value2", vec![DataType::String("X".to_string()), DataType::String("Y".to_string())]),
            ]
        );

        assert_relation_eq!(result_relation, expected_relation);
    }

    #[test]
    fn test_merge_join() {
        let relation1 = create_test_relation(
            "relation1",
            vec![
                ("id", vec![DataType::Int(1), DataType::Int(2), DataType::Int(3)]),
                ("value1", vec![DataType::String("A".to_string()), DataType::String("B".to_string()), DataType::String("C".to_string())]),
            ]
        );

        let relation2 = create_test_relation(
            "relation2",
            vec![
                ("id", vec![DataType::Int(2), DataType::Int(3), DataType::Int(4)]),
                ("value2", vec![DataType::String("X".to_string()), DataType::String("Y".to_string()), DataType::String("Z".to_string())]),
            ]
        );

        let result_relation = relation1.merge_join(&relation2, "id", "id", |a, b| a == b).unwrap();
        result_relation.pretty_print();
        
        let expected_relation = create_test_relation(
            "relation1",
            vec![
                ("id", vec![DataType::Int(2), DataType::Int(3)]),
                ("value1", vec![DataType::String("B".to_string()), DataType::String("C".to_string())]),
                ("value2", vec![DataType::String("X".to_string()), DataType::String("Y".to_string())]),
            ]
        );

        assert_relation_eq!(result_relation, expected_relation);
    }

    #[test]
    fn test_hash_join() {
        let relation1 = create_test_relation(
            "relation1",
            vec![
                ("id", vec![DataType::Int(1), DataType::Int(2), DataType::Int(3)]),
                ("value1", vec![DataType::String("A".to_string()), DataType::String("B".to_string()), DataType::String("C".to_string())]),
            ]
        );

        let relation2 = create_test_relation(
            "relation2",
            vec![
                ("id", vec![DataType::Int(2), DataType::Int(3), DataType::Int(4)]),
                ("value2", vec![DataType::String("X".to_string()), DataType::String("Y".to_string()), DataType::String("Z".to_string())]),
            ]
        );

        let result_relation = relation1.hash_join(&relation2, "id", "id", |a, b| a == b).unwrap();

        let expected_relation = create_test_relation(
            "relation1",
            vec![
                ("id", vec![DataType::Int(2), DataType::Int(3)]),
                ("value1", vec![DataType::String("B".to_string()), DataType::String("C".to_string())]),
                ("value2", vec![DataType::String("X".to_string()), DataType::String("Y".to_string())]),
            ]
        );

        assert_relation_eq!(result_relation, expected_relation);
    }
}
