use criterion::{criterion_group, criterion_main, Criterion};
use rustdb::database::*;
use rustdb::dtype::*;
use rustdb::interface::*;

fn db_bench(c: &mut Criterion) {

    let mut db = Database::new("rustdb").unwrap();
    db.create_relation("Students").unwrap();
    db.load_from_csv("Students", "test_data.csv", ",", ["id", "first_name", "last_name", "email", "grade"].to_vec()).unwrap();

    let mut i = 0;
    let tbl = "Students";


    c.bench_function("load_csv", |b| {
        b.iter(|| {
            let name = i.to_string() + tbl;
            db.create_relation(name.as_str()).unwrap();
            db.load_from_csv(name.as_str(), "test_data.csv", ",", ["id", "first_name", "last_name", "email", "grade"].to_vec()).unwrap();
            i += 1;
        });
    });

    //c.bench_function("pretty_print", |b| {
    //    b.iter(|| {
    //        db.pretty_print_relation("Students").unwrap();
    //    });
    //});

    let predicate = |datum: &DataType| {
        if let DataType::Float(grade) = datum {
            *grade < 3.0
        } else {
            false
        }
    };

    c.bench_function("select", |b| {
        b.iter(|| {
            db.select_from_relation("Students", "grade", predicate).unwrap();
            
        });
    });

    c.bench_function("project", |b| {
        b.iter(|| {
            db.project_relation("Students", ["first_name", "last_name", "email"].to_vec()).unwrap();
        });
    });

    c.bench_function("sort", |b| {
        b.iter(|| {
            db.sort_relation("Students", "grade", Order::Asc).unwrap();
        });
    });

    c.bench_function("agg", |b| {
        b.iter(|| {
            db.aggregate("Students", "grade", Aggregation::Average).unwrap();
        });
    });

    c.bench_function("idx", |b| {
        b.iter(|| {
            db.create_index("Students", "id").unwrap();
        });
    });

    c.bench_function("nlj", |b| {
        b.iter(|| {
            db.join("Students", "first_name", "Students", "first_name", |a, b| a == b, JoinType::NestedLoop).unwrap();
        });
    });

    c.bench_function("mej", |b| {
        b.iter(|| {
            db.join("Students", "first_name", "Students", "first_name", |a, b| a == b, JoinType::MergeJoin).unwrap();
        });
    });

    c.bench_function("hsj", |b| {
        b.iter(|| {
            db.join("Students", "first_name", "Students", "first_name", |a, b| a == b, JoinType::HashJoin).unwrap();
        });
    });

}

criterion_group!(benches, db_bench);
criterion_main!(benches);