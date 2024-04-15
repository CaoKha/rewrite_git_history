## Usage
### Prerequisites
#### prepare-tables
In prepare-tables/src/main.rs line 84, 
change the filter for column target from "SD CT" to anything you want
```rust
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

```

#### push-to-git
- Put all the zip files into `zips/` folder at the project root.

- In push-to-git/src/main.rs line 76,
Change the path into your own path for the repository
```rust
    let repo_path = get_path("../legacy-to-git");
```
- In line 89, 
change "B132264R-A" to the reference you want to construct .git
```rust
    let lf_list = read_parquet_files_with_substring(&get_path("./parquets"), "B13264R-A");
```
- In line 94,
change the starting zip file location to your corresponding starting reference
```rust
    let mut default_path_to_zip = get_path("./zips/Sources B13264R-A.zip");
```
### Run the scripts
You should position yourself at the project root level (where the `Cargo.toml` is found),
run this command to prepare the tables:
```bash
cargo run --bin prepare-tables
```
then run this command to building .git
```bash
cargo run --bin push-to-git
```
