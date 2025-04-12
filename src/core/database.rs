use crate::interface::*;
use crate::errors::*;
use crate::dtype::*;

use std::collections::HashMap;

/// Main DBMS structure 
pub struct Database {
    /// map to access relations by name
    relations: HashMap<String, ColumnStoreRelation>,

    /// name of the database
    name: String,
}


// #################################### OPTIONAL
#[derive(Debug)]
enum SqlCommand {
    Select {
        columns: Vec<String>,
        table: String,
        where_clause: Option<(String, String)>,  // (column, value)
    },
}

fn parse_sql(query: &str) -> Result<SqlCommand, String> {
    let mut tokens = query.split_whitespace().collect::<Vec<&str>>();

    if tokens.is_empty() {
        return Err("Empty query".to_string());
    }

    if tokens[0].to_uppercase() != "SELECT" {
        return Err("Only SELECT queries are supported".to_string());
    }

    tokens.remove(0); // Remove "SELECT"

    let mut columns = Vec::new();
    while !tokens.is_empty() {
        let token = tokens.remove(0);
        if token.to_uppercase() == "FROM" {
            break;
        }
        columns.push(token.trim_end_matches(',').to_string());
    }

    if columns.is_empty() {
        return Err("Expected columns in SELECT clause".to_string());
    }

    if tokens.is_empty() {
        return Err("Expected table name".to_string());
    }

    let table = tokens.remove(0).to_string();

    let mut where_clause = None;

    while !tokens.is_empty() {
        match tokens.remove(0).to_uppercase().as_str() {
            "WHERE" => {
                if tokens.len() < 2 {
                    return Err("Invalid WHERE clause".to_string());
                }
                let column = tokens.remove(0).to_string();
                let value = tokens.remove(0).to_string();
                where_clause = Some((column, value));
            }
            _ => return Err("Unexpected token in query".to_string()),
        }
    }

    Ok(SqlCommand::Select {
        columns,
        table,
        where_clause,
    })
}

// #################################### 



impl Database {
    /// creates a new database with given name
    pub fn new(name: &str) -> std::io::Result<Self> {
        Ok(Database {
            relations: HashMap::new(),
            name: name.to_string(),
        })
    }

    pub fn execute_sql(&mut self, query: &str) -> Result<ColumnStoreRelation, String> {
        let command = parse_sql(query)?;

        match command {
            SqlCommand::Select {
                columns,
                table,
                where_clause,
            } => {
                let mut relation = self.relations.get(&table)
                    .ok_or_else(|| "Table not found".to_string())?
                    .clone();

                if let Some((column, value)) = where_clause {
                    relation = relation.select(&column, |d| d.to_string() == value)
                        .map_err(|e| format!("{:?}", e))?;
                }

                relation.project(columns.iter().map(String::as_str).collect())
                    .map_err(|e| format!("{:?}", e))
            },
        }
    }

    /// Adds a new relation to the database
    pub fn add_relation(&mut self, name: String, relation: ColumnStoreRelation) {
        // Collect keys and values into Vecs to solve the borrowing issue
        self.relations.insert(name.clone(), relation);
    }

    /// creates a new relation and inserts it into the hashmap
    pub fn create_relation(&mut self, name: &str) -> Result<(), RelationErrors> {
        if self.relations.contains_key(name) {
            Err(RelationErrors::RelationAlreadyExists)
        } else {
            let relation = ColumnStoreRelation::new(); // Create a new instance
            self.relations.insert(name.to_string(), relation); // Add it to the database
            Ok(())
        }
    }

    /// loads a csv-file, given by path into an existing relation
    pub fn load_from_csv(
        &mut self,
        name: &str,
        path: &str,
        delimiter: &str, 
        select_columns: Vec<&str>,
    ) -> Result<(), RelationErrors> {
        // check and get relation by name
        if let Some(relation) = self.relations.get_mut(name) {
            return relation.load_csv(path, name, delimiter, select_columns);
        }
        Err(RelationErrors::RelationNotFound)
    }

    /// filters rows from a relation based on a predicate
    pub fn select_from_relation<F>(
        &mut self,
        name: &str,
        column_name: &str,
        predicate: F,
    ) -> Result<ColumnStoreRelation, RelationErrors>
    where
        F: Fn(&DataType) -> bool,
    {
        if let Some(relation) = self.relations.get_mut(name) {
            return relation.select(column_name, predicate);
        }
        Err(RelationErrors::RelationNotFound)
    }

    /// projects a relation onto a subset of its columns
    pub fn project_relation(
        &mut self,
        name: &str,
        columns_to_keep: Vec<&str>,
    ) -> Result<ColumnStoreRelation, RelationErrors> {
        if let Some(relation) = self.relations.get(name) {
            return relation.project(columns_to_keep);
        }
        Err(RelationErrors::RelationNotFound)
    }

    /// prints the content of a given relation into a table-like format to cmd
    pub fn pretty_print_relation(&self, name: &str) -> Result<(), RelationErrors> {
        println!("{}.{}", self.name, name);
        if let Some(relation) = self.relations.get(name) {
            relation.pretty_print();
            Ok(())
        } else {
            Err(RelationErrors::RelationNotFound)
        }
    }

    /// calculates an aggregate function on a given function
    pub fn aggregate(&self, relation_name: &str, column_name: &str, aggregation: Aggregation) -> Result<DataType, RelationErrors> {
        match self.relations.get(relation_name) {
            Some(relation) => relation.aggr(column_name, aggregation),
            None => Err(RelationErrors::RelationNotFound),
        }
    }

    /// sorts a relation in ascending or descending order
    pub fn sort_relation(&mut self, relation_name: &str, column_name: &str, order: Order) -> Result<(), RelationErrors> {
        if let Some(relation) = self.relations.get_mut(relation_name) {
            relation.sort(column_name, order)
        } else {
            Err(RelationErrors::RelationNotFound)
        }
    }

    /// creates and index for a given relation and column
    pub fn create_index(&mut self, relation_name: &str, column_name: &str) -> Result<(), String> {
        let relation = self.relations.get_mut(relation_name)
            .ok_or_else(|| "Relation not found".to_string())?;
        relation.create_index(column_name)
    }

    /// joins two columns given by name and predicate
    pub fn join<F>(&mut self, r_name: &str, r_col: &str, s_name: &str, s_col: &str, predicate: F, jt: JoinType) -> Result<ColumnStoreRelation, RelationErrors> 
    where F: Fn(&DataType, &DataType) -> bool {
        let r = self.relations.get(r_name).unwrap();
        let s = self.relations.get(s_name).unwrap();
        match jt {
            JoinType::NestedLoop => {
                return r.nested_loop_join(s, r_col, s_col, predicate);
            },
            JoinType::MergeJoin => {
                return r.merge_join(s, r_col, s_col, predicate);
            },
            JoinType::HashJoin => {
                return r.hash_join(s, r_col, s_col, predicate);
            }
        }
    }
}