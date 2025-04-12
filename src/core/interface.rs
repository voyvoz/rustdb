use crate::errors::*;
use crate::dtype::*;

use std::collections::{BTreeMap, HashMap};

// In-memory representation of a table/relation
#[derive(Clone)]
pub struct ColumnStoreRelation {
    // Name and identifier of relation
    pub name: String,
    // Table fields and schema
    pub fields: HashMap<String, DataType>,
    // Stored data
    pub columns: HashMap<String, Vec<DataType>>,
    // Query helper
    pub select_columns: Vec<String>,
    /// Indexes
    pub indices: HashMap<String, BTreeMap<String, Vec<usize>>>,
}

/// available aggregate functions
pub enum Aggregation {
    Count,
    Sum,
    Min,
    Max,
    Average,
}

/// order for sort operator
pub enum Order {
    Asc,
    Desc,
}

/// available join algorithms
pub enum JoinType {
    NestedLoop,
    MergeJoin,
    HashJoin,
}

/// main interface for relations
pub trait Relation {
//############################# SESSION 1 ############################

    /// return the name of the relation
    fn get_table_name(&self) -> String;
    
    /// returns the column name of the relation
    fn get_select_columns(&self) -> &Vec<String>;

    // load the relation from csv file    
    fn load_csv(
        &mut self,
        path: &str,
        table_name: &str,
        delimiter: &str,
        select_columns: Vec<&str>,
    ) -> Result<(), RelationErrors>;

    // saves the relation into a csv file
    fn save(&self, path: &str) -> Result<(), RelationErrors>;

    /// returns the number of tuples stored in the relation
    fn num_tuples(&self) -> Result<usize, RelationErrors>;

    /// prints the content of the relation to cmd in a table-like format
    fn pretty_print(&self);

    /// adds a tuple to a given relation
    fn add_tuple(&mut self, tuple: Vec<DataType>) -> Result<(), RelationErrors>;


    /// deleted a tuple of a relation
    fn delete_tuple<F>(&mut self, column_name: &str, predicate: F) -> Result<usize, RelationErrors>
    where F: Fn(&DataType) -> bool;

    /// updates a tuple a given relation
    fn update_tuple<F, G>(&mut self, target_column: &str, filter_column: &str, predicate: F, update_func: G) -> Result<usize, RelationErrors>
    where
        F: Fn(&DataType) -> bool,
        G: Fn(&DataType) -> DataType;

//####################################################################


//############################# SESSION 2 ############################

    /// filter the relation by given predicate
    fn scan<F>(&mut self, select_columns: Vec<&str>, predicate: F) -> Result<ColumnStoreRelation, RelationErrors> where F: Fn(&DataType) -> bool;

    /// filters a relation by given predicate on a given column
    fn select<F>(&mut self, column_name: &str, predicate: F) -> Result<ColumnStoreRelation, RelationErrors>
        where F: Fn(&DataType) -> bool;

    /// returns a relation with only selected columns
    fn project(&self, columns_to_keep: Vec<&str>) -> Result<ColumnStoreRelation, RelationErrors>;

    /// execute an aggregate function on a given column
    fn aggr(&self, column_name: &str, aggregation: Aggregation) -> Result<DataType, RelationErrors>;

    /// sorts the relation by given column and order
    fn sort(&mut self, column_name: &str, order: Order) -> Result<(), RelationErrors>;

    /// creates and index for a given column
    fn create_index(&mut self, column_name: &str) -> Result<(), String>;

    /// filters the relation by using a previously created/exisitng index
    fn index_select<F>(&self, column_name: &str, predicate: F) -> Result<ColumnStoreRelation, RelationErrors>
    where F: Fn(&DataType) -> bool;

//####################################################################    


//############################# SESSION 3 ############################

    /// performs a nested loop join with another column
    fn nested_loop_join<F>(&self, other_column: &ColumnStoreRelation, r_col: &str, s_col: &str, predicate: F) -> Result<ColumnStoreRelation, RelationErrors>
    where F: Fn(&DataType, &DataType) -> bool;

    //// performs a merge join with another column
    fn merge_join<F>(&self, other_column: &ColumnStoreRelation, r_col: &str, s_col: &str, predicate: F) -> Result<ColumnStoreRelation, RelationErrors>
    where F: Fn(&DataType, &DataType) -> bool;

    /// performs a hash join with another column
    fn hash_join<F>(&self, other_column: &ColumnStoreRelation, r_col: &str, s_col: &str, predicate: F) -> Result<ColumnStoreRelation, RelationErrors>
    where F: Fn(&DataType, &DataType) -> bool;

//#################################################################### 
}