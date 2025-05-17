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