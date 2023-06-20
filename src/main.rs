use arrow::array::{ArrayRef, BooleanArray, StringArray, UInt16Array};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug)]
#[allow(non_snake_case)]
struct Vechicle {
    VIN: String,
    make: String,
    model: String,
    year: u16,
    owner: String,
    isRegistered: Option<bool>,
}

fn main() -> Result<()> {
    let mut vehicles: Vec<Vechicle> = Vec::new();
    if let Ok(lines) = read_lines("vehicles.json") {
        for line in lines {
            if let Ok(js) = line {
                let v: Vechicle = serde_json::from_str(&js)?;
                println!("{:?}", v);
                vehicles.push(v);
            }
        }
    }

    write(vehicles)?;
    read()?;
    read2()?;

    Ok(())
}

fn write(vehicles: Vec<Vechicle>) -> Result<()> {
    let vins = StringArray::from(
        vehicles
            .iter()
            .map(|v| v.VIN.clone())
            .collect::<Vec<String>>(),
    );

    let makes = StringArray::from(
        vehicles
            .iter()
            .map(|v| v.make.clone())
            .collect::<Vec<String>>(),
    );

    let models = StringArray::from(
        vehicles
            .iter()
            .map(|v| v.model.clone())
            .collect::<Vec<String>>(),
    );

    let years = UInt16Array::from(vehicles.iter().map(|v| v.year).collect::<Vec<u16>>());

    let owners = StringArray::from(
        vehicles
            .iter()
            .map(|v| v.owner.clone())
            .collect::<Vec<String>>(),
    );

    let registrations =
        BooleanArray::from(vehicles.iter().map(|v| v.isRegistered).collect::<Vec<_>>());

    let batch = RecordBatch::try_from_iter(vec![
        ("VIN", Arc::new(vins) as ArrayRef),
        ("make", Arc::new(makes) as ArrayRef),
        ("model", Arc::new(models) as ArrayRef),
        ("year", Arc::new(years) as ArrayRef),
        ("owner", Arc::new(owners) as ArrayRef),
        ("isRegistered", Arc::new(registrations) as ArrayRef),
    ])
    .unwrap();

    let file = File::create("vehicles.parquet").unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).expect("Unable to write batch");
    writer.close().unwrap();

    Ok(())
}

fn read() -> Result<()> {
    println!("Reading parquet file...");
    let file = File::open("vehicles.parquet").unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    println!("Converted arrow schema is: {}", builder.schema());

    let mut reader = builder.build().unwrap();

    let record_batch = reader.next().unwrap().unwrap();

    println!("Read {} records.", record_batch.num_rows());

    Ok(())
}

#[allow(non_snake_case)]
fn read2() -> Result<()> {
    println!("Reading parquet file into vector of structs...");
    let file = File::open("vehicles.parquet").unwrap();
    let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let record_batch_reader = arrow_reader.build().unwrap();

    let mut vehicles: Vec<Vechicle> = vec![];

    for maybe_batch in record_batch_reader {
        let record_batch = maybe_batch.unwrap();
        let VIN = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let make = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let model = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let year = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        let owner = record_batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let isRegistered = record_batch
            .column(5)
            .as_any()
            .downcast_ref::<BooleanArray>();

        for i in 0..record_batch.num_rows() {
            vehicles.push(Vechicle {
                VIN: VIN.value(i).to_string(),
                make: make.value(i).to_string(),
                model: model.value(i).to_string(),
                year: year.value(i),
                owner: owner.value(i).to_string(),
                isRegistered: isRegistered.map(|a| a.value(i)),
            });
        }
    }

    for vehicle in vehicles {
        println!("{:?}", vehicle);
    }

    Ok(())
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
