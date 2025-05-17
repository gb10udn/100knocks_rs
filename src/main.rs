use polars::prelude::*;

fn main() -> PolarsResult<()> {
    // サンプルCSVの代わりに、直接DataFrameを作成
    let df = df! [
        "name" => &["Alice", "Bob", "Charlie"],
        "age" => &[25, 32, 37]
    ]?;

    // 年齢が30以上の人をフィルタ
    let filtered = df.lazy()
        .filter(col("age").gt_eq(lit(30)))
        .collect()?;

    println!("{}", filtered);

    Ok(())
}
