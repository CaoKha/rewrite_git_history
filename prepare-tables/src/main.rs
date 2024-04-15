use calamine::{open_workbook, Reader, Xls};
use csv::Writer;
use polars::datatypes::DataType;
use polars::io::parquet::ParquetWriter;
use polars::lazy::dsl::{col, lit, when};
use polars::prelude::*;
use smartstring::SmartString;
use std::error::Error;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::{env, i64};

enum LogicielType {
    Code,
    Etude,
}

trait AddColumn {
    fn add_reference_column(&mut self, lf_name: LogicielType);
}

fn get_path<P: AsRef<Path>>(path: P) -> PathBuf {
    if cfg!(target_os = "windows")
    {
        let path_buf = path.as_ref().to_path_buf();
        // Convert path to Windows style if the OS is Windows
        if let Some(p) = path_buf.to_str() {
            if p.contains('/') {
                return PathBuf::from(p.replace("/", "\\"));
            }
        }
        return path_buf;
    }

    // For non-Windows systems, return the input path as it is
    path.as_ref().to_path_buf()
}

fn main() -> Result<(), Box<dyn Error>> {
    env::set_var("RUST_BACKTRACE", "1");
    let excel_code = get_path("./xls/08122023_Logiciel_codifié.xls");
    let excel_etude = get_path("./xls/08122023_Logiciel_Etude.xls");
    let excel_ct_code = get_path("./xls/08122023_CT_Codified_Software.xls");
    let csv_folder = get_path("./csv");
    let csv_code = get_path("./csv/logiciel_code.csv"); // Output folder for the first Excel file
    let csv_etude = get_path("./csv/logiciel_etude.csv"); // Output folder for the second Excel file
    let csv_ct_code = get_path("./csv/logiciel_ct_code.csv");

    // Create csv directory if it doesn't exist
    if !csv_folder.exists() {
        fs::create_dir(csv_folder)?;
    }

    // Convert first Excel file to CSV
    convert_excel_to_csv(&excel_code, &csv_code)?;
    convert_excel_to_csv(&excel_etude, &csv_etude)?;
    convert_excel_to_csv(&excel_ct_code, &csv_ct_code)?;

    // DataFrame Schema to parse CSV into correct type
    let mut schema: Schema = Schema::new();
    schema.with_column(SmartString::from("Creation Date"), DataType::String);
    schema.with_column(SmartString::from("Archive Date"), DataType::String);
    schema.with_column(SmartString::from("Study Number"), DataType::String);
    schema.with_column(SmartString::from("Expedition Date"), DataType::String);

    let mut lf_code = LazyCsvReader::new(csv_code)
        .has_header(true)
        .with_dtype_overwrite(Some(&schema))
        .finish()?;
    let mut lf_ct_code = LazyCsvReader::new(csv_ct_code)
        .has_header(true)
        .with_dtype_overwrite(Some(&schema))
        .finish()?;
    let mut lf_etude = LazyCsvReader::new(csv_etude)
        .has_header(true)
        .with_dtype_overwrite(Some(&schema))
        .finish()?;

    // Add reference column
    lf_code.add_reference_column(LogicielType::Code);
    lf_ct_code.add_reference_column(LogicielType::Code);
    lf_etude.add_reference_column(LogicielType::Etude);

    // remove unnecessary columns
    let df_etude = lf_etude
        .filter(col("Target").str().contains(lit("SD CT"), false))
        .select([col("*").exclude(["Préf", "Number"])])
        .collect()?;
    let df_code = lf_code
        .filter(col("Target").str().contains(lit("SD CT"), false))
        .select([col("*").exclude(["Software P/N", "Version"])])
        .collect()?;
    let df_ct_code = lf_ct_code
        .filter(col("Target").str().contains(lit("SD CT"), false))
        .select([col("*").exclude(["Software P/N", "Version"])])
        .collect()?;

    // Construct final DataFrame
    let df_code = df_code.vstack(&df_ct_code)?;
    let df = df_code.vstack(&df_etude)?.with_row_count("Id", None)?;

    // Sort by Creation/Archive Date in descending order
    let mut df = df
        .lazy()
        .sort(
            "Creation Date",
            SortOptions {
                descending: true,
                nulls_last: true,
                multithreaded: true,
                maintain_order: false,
            },
        )
        .collect()?;

    // Optional: Make a relation parquet file to see the first link
    let mut df_relation = df
        .clone()
        .lazy()
        .group_by([col("Based On")])
        .agg([col("Reference").alias("References")])
        .collect()?;

    // Delete old .parquet files
    let parquets_folder_path = get_path("./parquets");
    match delete_parquet_files_in_directory(&parquets_folder_path) {
        Ok(_) => println!("All .parquet files deleted successfully"),
        Err(e) => eprintln!("Error deleting files: {}", e),
    }

    // Optional: Create original DataFrame 
    let original_parquet_path = get_path("./parquets/original.parquet");
    let mut file_original = File::create(original_parquet_path).unwrap();
    ParquetWriter::new(&mut file_original)
        .finish(&mut df)
        .unwrap();
    // Optional: First Link parquet files
    let relation_parquet_path = get_path("./parquets/relation.parquet");
    let mut file_relation = File::create(relation_parquet_path).unwrap();
    ParquetWriter::new(&mut file_relation)
        .finish(&mut df_relation)
        .unwrap();

    // Generate link lists DataFrame and write them into .parquet files
    let linked_lists = create_linked_lists(&df);
    let mut base_refs: Vec<&str> = Vec::new();

    for (index, list) in linked_lists.iter().enumerate() {
        let reference = list
            .column("Reference")
            .unwrap()
            .str()
            .unwrap()
            .get(0)
            .unwrap_or("");
        if !base_refs.contains(&reference) {
            base_refs.push(reference);
        }
        
        let file_name = get_path(format!("./parquets/{}_{}.parquet", reference, index + 1));

        let mut file = File::create(&file_name).expect("cannot create parquet file");
        ParquetWriter::new(&mut file)
            .finish(&mut list.clone())
            .expect("cannot write parquet file");
        println!("Linked List {}_{} is created", reference, index + 1);
    }

    vec_to_csv(&base_refs, &get_path("./csv/base-references.csv"))?;

    Ok(())
}

impl AddColumn for LazyFrame {
    fn add_reference_column(&mut self, lf_name: LogicielType) {
        match lf_name {
            LogicielType::Etude => {
                *self = self
                    .clone()
                    .with_columns([(col("Préf") * lit(1000000) + col("Number"))
                        .cast(DataType::String)
                        .alias("Reference")]);
            }

            LogicielType::Code => {
                *self = self
                    .clone()
                    .with_columns([when(col("Version").is_not_null())
                        .then(col("Software P/N") + lit("-") + col("Version"))
                        .otherwise(col("Software P/N"))
                        .cast(DataType::String)
                        .alias("Reference")]);
            }
        }
    }
}

fn convert_excel_to_csv(excel_path: &PathBuf, csv_path: &PathBuf) -> Result<(), Box<dyn Error>> {
    // Read data from Excel file starting from row 5 (header row)
    let mut excel: Xls<_> = open_workbook(excel_path)?;
    let sheet_names = excel.sheet_names().to_owned();
    let range = excel
        .worksheet_range(&sheet_names[0])
        .expect("Sheet not found");

    // Create a CSV writer
    let csv_file = File::create(&csv_path)?;
    let mut csv_writer = Writer::from_writer(csv_file);

    // Write CSV headers for the first 18 columns
    let headers: Vec<String> = range
        .rows()
        .skip(5) // Start from row 6 (0-based index)
        .next()
        .unwrap_or(&[])
        .iter()
        .take(18) // Take only the first 18 columns
        .map(|cell| format!("{}", cell))
        .collect();
    csv_writer.write_record(&headers)?;

    // Write CSV data for the first 18 columns starting from row 5
    for row in range.rows().skip(6) {
        let csv_row: Vec<String> = row
            .iter()
            .take(18) // Take only the first 18 columns
            .map(|cell| format!("{}", cell))
            .collect();
        csv_writer.write_record(&csv_row)?;
    }

    println!("CSV Conversion successful: {}", csv_path.display());
    Ok(())
}

// The dataframe is sorted by date in descending order (most recent day -> the oldest day)
// Begin linking only if no other versions based on this version have been created subsequently
fn can_start_linked_list(df: &DataFrame, current_index: usize) -> bool {
    let reference = df
        .column("Reference")
        .unwrap()
        .str()
        .unwrap()
        .get(current_index);
    let based_on_col = df.column("Based On").unwrap().str().unwrap();
    for i in 0..current_index {
        if based_on_col.get(i) == reference {
            return false;
        }
    }
    true
}

fn create_linked_lists(df: &DataFrame) -> Vec<DataFrame> {
    let mut linked_lists: Vec<DataFrame> = Vec::new();
    for i in 0..df.height() {
        if can_start_linked_list(df, i) {
            let mut current_index = i;
            let mut current_list = DataFrame::default();
            // Go from top to bottom of the table, linking rows that satisfy this condition:
            // Reference value of next row = Based On value of current row
            while current_index < df.height() {
                let current_row = df.slice(current_index as i64, 1);
                current_list = current_list
                    .vstack(&current_row)
                    .expect("can't stack current_row into current_list");
                let based_on = current_row
                    .column("Based On")
                    .expect("column Based On not found")
                    .str()
                    .expect("Based On column is not of type String")
                    .get(0);
                let next_index = (0..df.height())
                    .filter(|&idx| idx != current_index)
                    .find(|&j| based_on == df.column("Reference").unwrap().str().unwrap().get(j));
                // println!("{:?}", next_index);

                if let Some(next_index) = next_index {
                    current_index = next_index
                } else {
                    // If no next row satisfies the condition, push the current_list to linked_lists
                    // Only push to linked_lists if there is at least 2 element in the current link
                    // list
                    if current_list.height() >= 2 {
                        // Reverse the link list (date from old -> recent) to make the root version
                        // go to the first row
                        linked_lists.push(current_list.reverse().clone());
                    }
                    break;
                }
            }
        }
    }
    linked_lists
}

fn delete_parquet_files_in_directory(directory_path: &PathBuf) -> std::io::Result<()> {
    let paths = fs::read_dir(directory_path)?;
    for path in paths {
        if let Ok(entry) = path {
            if let Some(extension) = entry.path().extension() {
                if extension == "parquet" {
                    fs::remove_file(entry.path())?;
                    println!("Deleted file: {:?}", entry.path().display());
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn vec_to_csv<T: ToString>(data: &[T], file_path: &PathBuf) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_path)?;
    let mut writer = Writer::from_writer(file);
    for value in data {
        writer.write_record(&[value.to_string()])?;
    }
    writer.flush()?;
    println!("Data saved to CSV file: {}", file_path.display());
    Ok(())
}

