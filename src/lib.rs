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
        .collect()?
        .select(["sales_ymd", "customer_id", "product_cd", "amount"])?;

    println!("{:?}", df);
    Ok(())
}


pub fn p_003() -> PolarsResult<()> {
    let file = "./data/receipt.csv";
    let mut df = LazyCsvReader::new(file)
        .with_n_rows(Some(10))
        .finish()?
        .collect()?
        .select(["sales_ymd", "customer_id", "product_cd", "amount"])?;
         
    let df = df.rename("sales_ymd", "sales_ymdsales_ymd".into())?;  // INFO: 250517 .rename() は、メソッドチェーンの中で使用不可。

    println!("{:?}", df);

    Ok(())
}