use polars::prelude as pl;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

enum GitState {
    AlreadyInit,
    FirstInit,
}

enum NewBranch {
    Created,
    NotCreated,
}

struct GitInfo {
    commit_time: String,
    commit_message: String,
    author_name: String,
    author_email: String,
    branch_name: String
}

impl GitInfo {
    fn new(
        commit_time: String,
        commit_message: String,
        author_name: String,
        author_email: String,
        branch_name: String,
    ) -> Self {
        GitInfo {
            commit_time,
            commit_message,
            author_name,
            author_email,
            branch_name
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct VisitedReference {
    reference: String,
    commit_hash: String,
}

impl VisitedReference {
    fn new(reference: String, commit_hash: String) -> Self {
        VisitedReference {
            reference,
            commit_hash,
        }
    }
}

/// a wrapper for windows to get path of a file or a directory
fn get_path<P: AsRef<Path>>(path: P) -> PathBuf {
    if cfg!(target_os = "windows") {
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

/// main logic of the script
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repo_path = get_path("../legacy-to-git");
    // First delete the old repo folder
    match delete_folder(&repo_path) {
        Ok(_) => println!("{} is deleted successfully!", repo_path.display()),
        Err(e) => println!("Error deleting folder {}: {}", repo_path.display(), e),
    };
    // git init
    match create_folder_and_init_git_repo(&repo_path) {
        Ok(GitState::FirstInit) => println!("Git repo initialized successfully!"),
        Ok(GitState::AlreadyInit) => println!("Git repo already initialized!"),
        Err(e) => eprintln!("Failed to initialize Git repo: {}", e),
    };
    // Read the Parquet file into a DataFrame
    let lf_list = read_parquet_files_with_substring(&get_path("./parquets"), "B13264R-A");
    // Make a list of unique reference that has been visited, this will help to create new branch
    // if we visit a new reference. It also help to create a new branch from previous visited
    // reference
    let mut reference_list: HashSet<VisitedReference> = HashSet::new();
    let mut default_path_to_zip = get_path("./zips/Sources B13264R-A.zip");

    // Loop through the list
    for (lf_index, lf) in lf_list.into_iter().enumerate() {
        let reference_col = lf.clone().collect()?.column("Reference")?.str()?.clone();
        if lf_index == 0 {
            // first branch
            println!("Creating first branch...");
            for (ref_index, reference) in reference_col.into_iter().enumerate() {
                let unique_reference = reference.expect("a reference is empty").to_owned();
                let date = lf
                    .clone()
                    .collect()?
                    .column("Creation Date")?
                    .str()?
                    .get(ref_index)
                    .unwrap_or("")
                    .to_owned();
                let comment = lf
                    .clone()
                    .collect()?
                    .column("Comments")?
                    .str()?
                    .get(ref_index)
                    .unwrap_or("no_comment_found")
                    .to_owned();
                let better_comment = format!("[{}] {}", unique_reference, comment);
                let author_name = lf
                    .clone()
                    .collect()?
                    .column("Author")?
                    .str()?
                    .get(ref_index)
                    .unwrap_or("no_author_found")
                    .to_owned();
                let author_email =
                    author_name.replace(" ", "").to_lowercase().clone() + "@allianz.com";
                let git_info = GitInfo::new(date, better_comment, author_name, author_email, unique_reference);

                // First init, create a initial branch with message `first init`
                if ref_index == 0 {
                    git_init_and_switch_to_first_branch(
                        &repo_path,
                        &git_info,
                    )?;
                }
                let commit_id = zip_to_git(
                    &repo_path,
                    &git_info,
                    &mut default_path_to_zip,
                );
                let reference_element = VisitedReference::new(git_info.branch_name, commit_id);
                reference_list.insert(reference_element);
            }
        } else {
            // associate branches
            let mut previous_commit_hash = String::from("");
            println!("Creating branch number {}...", lf_index + 1);
            let mut new_branch_created = NewBranch::NotCreated;
            for (ref_index, reference) in reference_col.into_iter().enumerate() {
                let unique_reference = reference.expect("a reference is empty").to_owned();
                let date = lf
                    .clone()
                    .collect()?
                    .column("Creation Date")?
                    .str()?
                    .get(ref_index)
                    .unwrap_or("")
                    .to_owned();
                let comment = lf
                    .clone()
                    .collect()?
                    .column("Comments")?
                    .str()?
                    .get(ref_index)
                    .unwrap_or("no_comments_found")
                    .to_owned();
                let better_comment = format!("[{}] {}", unique_reference, comment);
                let author_name = lf
                    .clone()
                    .collect()?
                    .column("Author")?
                    .str()?
                    .get(ref_index)
                    .unwrap_or("no_author_found")
                    .to_owned();
                let author_email = author_name.replace(" ", "").clone() + "@allianz.com";
                let git_info = GitInfo::new(date, better_comment, author_name, author_email, unique_reference.to_owned());
                if !contains_substring(
                    &reference_list,
                    &unique_reference,
                    &mut previous_commit_hash,
                ) {
                    match new_branch_created {
                        NewBranch::Created => {
                            let commit_id = zip_to_git(
                                &repo_path,
                                &git_info,
                                &mut default_path_to_zip,
                            );
                            reference_list
                                .insert(VisitedReference::new(unique_reference, commit_id));
                        }
                        NewBranch::NotCreated => {
                            // create a branch from previous existing branch
                            git_create_and_switch_to_new_branch_from_commit(
                                unique_reference.as_str(),
                                &repo_path,
                                &previous_commit_hash,
                            )?;
                            let commit_id = zip_to_git(
                                &repo_path,
                                &git_info,
                                &mut default_path_to_zip,
                            );
                            reference_list
                                .insert(VisitedReference::new(unique_reference, commit_id));
                            new_branch_created = NewBranch::Created;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn read_parquet_files_with_substring(parquet_path: &Path, substring: &str) -> Vec<pl::LazyFrame> {
    let mut frames = Vec::new();

    for entry in WalkDir::new(parquet_path)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if let Some(file_path) = entry.path().to_str() {
            if file_path.contains(substring) && file_path.ends_with(".parquet") {
                if let Ok(frame) = pl::LazyFrame::scan_parquet(file_path, Default::default()) {
                    frames.push(frame);
                }
            }
        }
    }
    frames
}

fn create_folder_and_init_git_repo(repository_path: &Path) -> Result<GitState, git2::Error> {
    // Check if the .git folder already exists in the repository_path
    let git_folder_path = repository_path.join(".git");
    if git_folder_path.exists() {
        println!("Git folder already exists in {}", repository_path.display());
        return Ok(GitState::AlreadyInit);
    }

    std::fs::create_dir_all(repository_path).expect("Could not create the directory");
    git2::Repository::init_opts(repository_path, &git2::RepositoryInitOptions::new())?;
    println!(
        "Git repository is initialized in {}",
        repository_path.display()
    );
    Ok(GitState::FirstInit)
}

/// Function to find and return the path of the ZIP file containing a specific value in its name
fn find_zip_file(value: &str, sources_folder: &Path) -> Option<std::path::PathBuf> {
    for entry in WalkDir::new(sources_folder)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if let Some(zip_path) = entry.path().to_str() {
            if zip_path.to_lowercase().contains(".zip") && zip_path.contains(value) {
                return Some(entry.path().to_path_buf());
            }
        }
    }
    None
}

/// Find location of the zip file in `zips/` folder, if not found use the default zip folder
fn zip_to_git(
    repo_path: &Path,
    git_info: &GitInfo,
    default_path_to_zip: &mut PathBuf,
) -> String {
    let zips_folder = get_path("./zips");
    match find_zip_file(&git_info.branch_name, &zips_folder) {
        Some(zip_path) => {
            *default_path_to_zip = zip_path;
            println!("Extracting {} ", default_path_to_zip.display());
        }
        None => {
            println!(
                "Can't find any files with reference: {} use the previous zip folder: {}",
                &git_info.branch_name,
                default_path_to_zip.display()
            );
            println!(
                "Extracting previous zip file {} ",
                default_path_to_zip.display()
            );
        }
    }

    extract_zip_to_repo(default_path_to_zip, &repo_path).expect("Can't extract zip file");
    git_add_all(&repo_path).expect("Git-add error");
    git_commit(&repo_path, &git_info).expect("Can't get a commit id")
}

fn extract_zip_to_repo(zip_file: &Path, extract_dir: &Path) -> Result<(), std::io::Error> {
    let file = std::fs::File::open(zip_file)?;
    let mut archive = zip::ZipArchive::new(file)?;

    let temp_path = get_path("./temp");
    std::fs::create_dir_all(&temp_path)?;

    archive.extract(&temp_path)?;
    match get_project_root(&temp_path) {
        Ok(project_root_path) => {
            println!("Project root found at: {}", project_root_path.display());
            delete_folder_contents_except_git(extract_dir)?;
            copy_dir_all(&project_root_path, extract_dir)?;
        }
        Err(err) => panic!("Error: {}", err),
    }

    std::fs::remove_dir_all(temp_path)?;

    Ok(())
}

fn get_project_root(dir_path: &Path) -> Result<PathBuf, std::io::Error> {
    let mut current_path = dir_path.to_path_buf();

    loop {
        let folder_paths: Vec<PathBuf> = std::fs::read_dir(&current_path)?
            .filter_map(|entry| entry.ok()) // Filter out potential errors
            .filter(|entry| entry.path().is_dir()) // Filter only directories
            .map(|entry| entry.path())
            .collect();

        match folder_paths.len() {
            1 => {
                // Update the current path to the single child folder found
                current_path = folder_paths[0].clone();
            }
            _ => break, // Break the loop if more than one folder or no folders found
        }
    }

    Ok(current_path)
}

fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> std::io::Result<()> {
    std::fs::create_dir_all(&dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if entry.file_name() != ".git" {
            if ty.is_dir() {
                copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
            } else {
                std::fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
            }
        }
    }
    Ok(())
}

fn delete_folder(path: &Path) -> std::io::Result<()> {
    std::fs::remove_dir_all(path)?;
    Ok(())
}

fn git_add_all(repo_path: &Path) -> Result<(), git2::Error> {
    let mut opts = git2::StatusOptions::new();
    opts.include_untracked(true);
    let repo = git2::Repository::open(repo_path)?;
    let mut index = repo.index()?;
    index.add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)?;
    index.write()?;
    Ok(())
}

fn git_commit(repo_path: &Path, git_info: &GitInfo) -> Result<String, git2::Error> {
    let repo = git2::Repository::open(repo_path)?;
    let commit_unix_time =
        excel_date_to_unix_timestamp(git_info.commit_time.parse::<f64>().unwrap_or(0.0));
    let signature = git2::Signature::new(
        &git_info.author_name,
        &git_info.author_email,
        &git2::Time::new(commit_unix_time, 0),
    )?;
    let tree_id = repo.index()?.write_tree()?;
    let tree = repo.find_tree(tree_id)?;
    let head_commit = repo.head()?.peel_to_commit()?;
    let commit_id = repo.commit(
        Some("HEAD"),
        &signature,
        &signature,
        &git_info.commit_message,
        &tree,
        &[&head_commit],
    )?;
    let commit = repo.find_object(commit_id, Some(git2::ObjectType::Commit))?;
    repo.tag(&git_info.branch_name, &commit, &signature, "", false)?;
    Ok(commit_id.to_string())
}

fn git_create_and_switch_to_new_branch_from_commit(
    branch_name: &str,
    repo_path: &Path,
    commit_hash: &str,
) -> Result<(), git2::Error> {
    let repo = git2::Repository::open(repo_path)?;
    let commit_object = repo.revparse_single(commit_hash)?;
    let commit = commit_object.peel_to_commit()?;
    let reference_name = format!("refs/heads/{}", branch_name);
    repo.branch(&branch_name, &commit, false)?;
    let obj = repo.revparse_single(&reference_name).unwrap();
    repo.checkout_tree(&obj, None)?;
    repo.set_head(&reference_name)?;
    println!(
        "Branch {} is created. Main branch is switched to that branch.",
        branch_name
    );
    Ok(())
}

fn git_init_and_switch_to_first_branch(
    repo_path: &Path,
    git_info: &GitInfo,
) -> Result<(), git2::Error> {
    let repo = git2::Repository::open(repo_path)?;
    let tree_id = repo.index()?.write_tree()?;
    let tree = repo.find_tree(tree_id)?;
    let commit_unix_time =
        excel_date_to_unix_timestamp(git_info.commit_time.parse::<f64>().unwrap_or(0.0));
    let signature = git2::Signature::new(
        &git_info.author_name,
        &git_info.author_email,
        &git2::Time::new(commit_unix_time, 0),
    )?;

    repo.commit(
        Some("HEAD"),
        &signature,
        &signature,
        "First init",
        &tree,
        &[],
    )?;
    repo.branch(&git_info.branch_name, &repo.head()?.peel_to_commit()?, false)?;
    let reference_name = format!("refs/heads/{}", &git_info.branch_name);
    let obj = repo.revparse_single(&reference_name).unwrap();
    repo.checkout_tree(&obj, None)?;
    repo.set_head(&reference_name)?;
    println!(
        "Branch {} is created. Main branch is switched to that branch.",
        &git_info.branch_name
    );
    Ok(())
}

fn contains_substring(
    set: &HashSet<VisitedReference>,
    substring: &str,
    previous_commit_hash: &mut String,
) -> bool {
    // Iterate through the HashSet
    for item in set {
        // Check if the reference field contains the substring
        if item.reference.contains(substring) {
            *previous_commit_hash = item.commit_hash.clone();
            return true;
        }
    }
    false
}

fn delete_folder_contents_except_git(path: &Path) -> Result<(), std::io::Error> {
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let file_path = entry.path();

            if file_path.file_name() != Some(std::ffi::OsStr::new(".git")) {
                if file_type.is_dir() {
                    delete_folder_contents_except_git(&file_path)?;
                    std::fs::remove_dir(&file_path)?;
                } else {
                    std::fs::remove_file(&file_path)?;
                }
            }
        }
    }
    Ok(())
}

/// Function to convert Excel serial date to Unix timestamp
fn excel_date_to_unix_timestamp(serial_date: f64) -> i64 {
    // Excel serial date (days since 1900-01-01)
    let excel_epoch = chrono::NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
    let days = serial_date as i64;

    // Calculate the date from the serial number
    let date = excel_epoch + chrono::Duration::days(days - 2); // Subtracting 2 days for Excel's date epoch adjustment

    // Unix epoch (1970-01-01)
    let unix_epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

    // Calculate the duration between Unix epoch and the date
    let duration = date.signed_duration_since(unix_epoch);

    // Return the total number of seconds as Unix timestamp
    duration.num_seconds()
}

