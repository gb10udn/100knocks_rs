use polars::prelude::*;
use std::fs::File;


pub fn p_001() -> PolarsResult<()> {
    let file = "./data/receipt.csv";
    let df = LazyCsvReader::new(file)
        .with_has_header(true)
        .with_n_rows(Some(10))
        .finish()?
        .collect();

    println!("{:?}", df);
    Ok(())
}

pub fn p_002() -> PolarsResult<()> {
    let file = "./data/receipt.csv";
    let df = LazyCsvReader::new(file)
        .with_n_rows(Some(10))
        .finish()?
        .select([col("sales_ymd"), col("customer_id"), col("product_cd"), col("amount")])  //INFO: 250517 .collect() 前で呼び出すと最適化がかかるみたい。
        .collect()?;

    println!("{:?}", df);
    Ok(())
}

pub fn p_003() -> PolarsResult<()> {
    let file = "./data/receipt.csv";
    let df = LazyCsvReader::new(file)
        .with_n_rows(Some(10))
        .finish()?
        .select([
            col("sales_ymd").alias("sales_date"),  // INFO: 250518 .alias() で名称の変更ができる。.rename() はコメントアウトで残してるが制約多いので、良くないかも？
            col("customer_id"),
            col("product_cd"),
            col("amount")
        ])
        .collect()?;
         
    // let df = df.rename("sales_ymd", "sales_ymdsales_ymd".into())?;  // INFO: 250517 .rename() は、メソッドチェーンの中で使用不可。

    println!("{:?}", df);

    Ok(())
}

pub fn p_004() -> PolarsResult<()> {
    let file = "./data/receipt.csv";
    
    let df = LazyCsvReader::new(file)
        .with_has_header(true)
        .finish()?
        .select([col("sales_ymd"), col("customer_id"), col("product_cd"), col("amount")])
        .filter(col("customer_id").eq(lit("CS018205000001")))
        .collect()?;

    println!("{:?}", df);
    Ok(())
}

pub fn p_005() -> PolarsResult<()> {
    let file = "./data/receipt.csv";

    let mut df = LazyCsvReader::new(file)
        .finish()?
        .select([col("sales_ymd"), col("customer_id"), col("product_cd"), col("amount")])
        .filter(col("customer_id").eq(lit("CS018205000001")))  // INFO: 250517 lit がないと落ちる。Lazy 特有？lit や col は polars では付けるとしておけばいいかな？
        .filter(col("amount").gt_eq(lit(1000)))
        .collect()?;
    println!("{:?}", df);

    // INFO: 250517 csv でデータを吐いて、中身を確認する。
    // ref: https://polars-ja.github.io/docs-ja/user-guide/getting-started/#_2
    CsvWriter::new(File::create("./misc/p_005.csv")?)
        .include_header(true)
        .include_bom(true)  // INFO: 250517 utf8 で吐き出すと思うので、bom 付けないとエクセルで開けないハズ。
        .finish(&mut df)?;

    Ok(())
}

pub fn p_006() -> PolarsResult<()> {
    let df = LazyCsvReader::new("./data/receipt.csv")
        .finish()?
        .select([col("sales_ymd"), col("customer_id"), col("product_cd"), col("quantity"), col("amount")])
        .filter(col("customer_id").eq(lit("CS018205000001")))
        .filter(col("amount").gt_eq(lit(1000)).or(col("quantity").gt_eq(lit(4))))
        .collect()?;

    println!("{:?}", df);

    Ok(())
}

pub fn p_007() -> PolarsResult<()> {
    let df = LazyCsvReader::new("./data/receipt.csv")
        .finish()?
        .select([col("sales_ymd"), col("customer_id"), col("product_cd"), col("quantity"), col("amount")])
        .filter(col("customer_id").eq(lit("CS018205000001")))
        .filter(col("amount").gt_eq(lit(1000)).and(col("amount").lt_eq(lit(2000))))
        .collect()?;

    println!("{:?}", df);

    Ok(())
}

pub fn p_008() -> PolarsResult<()> {
    let df = LazyCsvReader::new("./data/receipt.csv")
        .finish()?
        .select([
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("amount")
        ])
        .filter(col("customer_id").eq(lit("CS018205000001")))
        .filter(col("product_cd").neq(lit("P071401019")))  // INFO: 250518 問題文には "P071401019" と書いてあるが、この product_cd を含むものが入っていない、、、？
        .collect()?;

    println!("{:?}", df);

    Ok(())
}

pub fn p_009() -> PolarsResult<()> {
    let df = LazyCsvReader::new("./data/store.csv")
        .with_n_rows(Some(10))
        .finish()?
        .filter(col("prefecture_cd").neq(lit(13)))
        .filter(col("floor_area").lt_eq(lit(900)))
        .collect();

    println!("{:?}", df);

    Ok(())
}