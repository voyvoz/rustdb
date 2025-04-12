use rustdb::database::Database;

fn main() {
    let mut db = Database::new("rustdb").unwrap();
    db.create_relation("Students").unwrap();
    db.load_from_csv("Students", "test_data.csv", ",", ["id", "first_name", "last_name", "email", "grade"].to_vec()).unwrap();
    db.pretty_print_relation("Students").unwrap();

    return;
}