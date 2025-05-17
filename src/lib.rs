use polars::prelude::*;

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
    let mut df = LazyCsvReader::new(file)
        .with_n_rows(Some(10))
        .finish()?
        .select([col("sales_ymd"), col("customer_id"), col("product_cd"), col("amount")])
        .collect()?;
         
    let df = df.rename("sales_ymd", "sales_ymdsales_ymd".into())?;  // INFO: 250517 .rename() は、メソッドチェーンの中で使用不可。

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