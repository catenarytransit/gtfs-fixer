use std::collections::HashMap; // To store header indices
use std::env;
use std::error::Error;
use std::fs::{remove_file, rename, File};
use std::io::{stdout, BufReader, BufWriter, Write}; // Added Write for flushing stdout
use std::path::{Path, PathBuf};
use csv::{ReaderBuilder, StringRecord, Writer};

// --- Configuration ---
// Names of the files within the GTFS directory
const STOPS_FILENAME: &str = "stops.txt";
const SHAPES_FILENAME: &str = "shapes.txt";
// Temporary file suffix
const TEMP_SUFFIX: &str = ".tmp";
// Target column names (case-insensitive comparison will be used)
const STOP_LAT_COLUMN_NAME: &str = "stop_lat";
const STOP_LON_COLUMN_NAME: &str = "stop_lon";
const SHAPE_LAT_COLUMN_NAME: &str = "shape_pt_lat";
const SHAPE_LON_COLUMN_NAME: &str = "shape_pt_lon";
// Number of decimal places for output coordinates
const COORDINATE_PRECISION: usize = 8;
// --- End Configuration ---

/// Attempts to parse a string potentially containing a floating-point number
/// (including scientific notation) and formats it to a fixed number of decimal places.
/// If parsing fails, it returns the original string.
///
/// # Arguments
/// * `value_str` - The string slice to parse and format.
///
/// # Returns
/// A `String` containing the formatted number or the original string on error.
fn format_coordinate(value_str: &str) -> String {
    match value_str.trim().parse::<f64>() {
        // Successfully parsed as f64 (handles standard and scientific notation)
        Ok(val) => format!("{:.prec$}", val, prec = COORDINATE_PRECISION),
        // If parsing fails, return the original string unchanged
        Err(_) => value_str.to_string(),
    }
}

/// Finds the indices of specified columns in a CSV header record.
/// Performs case-insensitive comparison.
///
/// # Arguments
/// * `headers` - The StringRecord containing the header row.
/// * `col_names` - A slice of strings representing the column names to find.
///
/// # Returns
/// A `Result` containing a `HashMap` mapping column names (lowercase) to their 0-based indices,
/// or an `Err` if any of the specified columns are not found.
fn find_column_indices(
    headers: &StringRecord,
    col_names: &[&str],
) -> Result<HashMap<String, usize>, Box<dyn Error>> {
    let mut indices = HashMap::new();
    // Create a map for quick lookup of header names (lowercase) and their indices
    let header_map: HashMap<_, _> = headers
        .iter()
        .enumerate()
        .map(|(i, name)| (name.trim().to_lowercase(), i))
        .collect();

    // Find the index for each required column name
    for &name in col_names {
        let lower_name = name.to_lowercase(); // Use lowercase for lookup and storage
        if let Some(&index) = header_map.get(&lower_name) {
            indices.insert(lower_name, index);
        } else {
            // If a required column is missing, return an error
            return Err(format!("Required column '{}' not found in header.", name).into());
        }
    }
    Ok(indices)
}

/// Generic function to process a GTFS file (like stops.txt or shapes.txt).
/// Reads the specified file, fixes coordinate formats in the given lat/lon columns,
/// and overwrites the original file using a temporary file. Finds columns dynamically.
///
/// # Arguments
/// * `gtfs_dir` - Path to the directory containing the GTFS files.
/// * `filename` - The name of the file to process (e.g., "stops.txt").
/// * `lat_col_name` - The name of the latitude column to fix.
/// * `lon_col_name` - The name of the longitude column to fix.
///
/// # Returns
/// `Ok(())` on success, or an `Err` containing the error information.
fn process_gtfs_file(
    gtfs_dir: &Path,
    filename: &str,
    lat_col_name: &str,
    lon_col_name: &str,
) -> Result<(), Box<dyn Error>> {
    // Construct full paths for input and temporary output files
    let input_path = gtfs_dir.join(filename);
    let temp_output_filename = format!("{}{}", filename, TEMP_SUFFIX);
    let temp_output_path = gtfs_dir.join(&temp_output_filename);

    println!("\nStarting processing of '{}'...", input_path.display());

    // --- Input File Handling ---
    if !input_path.exists() {
        // It's not necessarily an error if an optional file like shapes.txt doesn't exist
        println!(
            "Info: File '{}' not found in directory '{}'. Skipping processing.",
            filename,
            gtfs_dir.display()
        );
        return Ok(()); // Return Ok to allow processing of other files
    }
    let input_file = File::open(&input_path)?;
    let reader = BufReader::new(input_file);

    // Configure CSV reader
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true) // Read the first row as a header
        .from_reader(reader);

    // --- Temporary Output File Handling ---
    // Defer file creation until header is successfully read and columns found
    let temp_output_file: File;
    let mut csv_writer: Writer<BufWriter<File>>; // Declare writer

    // --- Header Processing & Column Index Finding ---
    let headers = csv_reader.headers()?.clone(); // Clone to own the data

    // Find the indices of the latitude and longitude columns dynamically
    let required_columns = [lat_col_name, lon_col_name];
    let column_indices = match find_column_indices(&headers, &required_columns) {
        Ok(indices) => indices,
        Err(e) => {
            // Specific error for column finding
            eprintln!(
                "Error finding columns in '{}': {}. Skipping processing.",
                input_path.display(),
                e
            );
            // No temporary file created yet, so no cleanup needed here
            return Err(e); // Propagate the error
        }
    };

    // Retrieve the specific indices (unwrap is safe here due to the check in find_column_indices)
    // Use lowercase for HashMap lookup
    let lat_col_idx = *column_indices.get(&lat_col_name.to_lowercase()).unwrap();
    let lon_col_idx = *column_indices.get(&lon_col_name.to_lowercase()).unwrap();

    println!(
        "Found Latitude column: '{}' (Index {})",
        headers.get(lat_col_idx).unwrap_or("N/A"), // Get original header name for display
        lat_col_idx
    );
    println!(
        "Found Longitude column: '{}' (Index {})",
        headers.get(lon_col_idx).unwrap_or("N/A"), // Get original header name for display
        lon_col_idx
    );

    // Now create the temporary file and writer
    temp_output_file = File::create(&temp_output_path)?;
    let writer = BufWriter::new(temp_output_file);
    csv_writer = Writer::from_writer(writer);

    // Write the original header to the temporary output file
    csv_writer.write_record(&headers)?;
    println!(
        "Header written to temporary file '{}'.",
        temp_output_path.display()
    );

    // --- Record Processing ---
    let mut processed_count = 0;
    let mut record = StringRecord::new(); // Reusable record

    // Iterate over each data record in the input file
    while csv_reader.read_record(&mut record)? {
        // Ensure the record has enough fields (robustness against malformed rows)
        if record.len() <= lat_col_idx || record.len() <= lon_col_idx {
             eprintln!(
                "\nWarning: Skipping malformed row {} ({} fields) in '{}'. Expected at least {} fields.",
                processed_count + 1, // +1 for 1-based row number (approx)
                record.len(),
                filename,
                std::cmp::max(lat_col_idx, lon_col_idx) + 1
            );
            continue; // Skip this row
        }

        let mut output_fields: Vec<String> = Vec::with_capacity(record.len());

        // Process each field, formatting coordinates based on the found indices
        for (index, field) in record.iter().enumerate() {
            let processed_field = if index == lat_col_idx || index == lon_col_idx {
                // If it's the dynamically found lat or lon column, format it
                format_coordinate(field)
            } else {
                // Otherwise, keep the field as is
                field.to_string()
            };
            output_fields.push(processed_field);
        }

        // Write the potentially modified record to the temporary output CSV
        csv_writer.write_record(&output_fields)?;
        processed_count += 1;

        // Optional: Progress indicator
        if processed_count % 5000 == 0 { // Adjusted frequency
            print!("\rProcessed {} records for {}...", processed_count, filename);
            stdout().flush()?; // Ensure the progress message is displayed immediately
        }
    }
    println!(
        "\rProcessed {} records for {}.      ", // Clear progress line
        processed_count, filename
    );

    // --- Finalisation ---
    // Ensure all buffered data is written to the temporary file
    csv_writer.flush()?;
    println!(
        "Successfully processed {} records from '{}' to temporary file.",
        processed_count, filename
    );

    // --- Replace Original File ---
    // Rename the temporary file to the original filename, overwriting it.
    rename(&temp_output_path, &input_path)?;
    println!(
        "Successfully replaced '{}' with the processed data.",
        input_path.display()
    );

    Ok(())
}

fn main() {
    // --- Argument Parsing ---
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <path_to_gtfs_directory>", args[0]);
        eprintln!("Example: {} /path/to/your/gtfs_feed/", args[0]);
        std::process::exit(1);
    }

    let gtfs_dir_path = PathBuf::from(&args[1]);

    // --- Directory Validation ---
    if !gtfs_dir_path.is_dir() {
        eprintln!(
            "Error: Provided path '{}' is not a valid directory.",
            gtfs_dir_path.display()
        );
        std::process::exit(1);
    }

    // --- Execute Processing for stops.txt ---
    let stops_result = process_gtfs_file(
        &gtfs_dir_path,
        STOPS_FILENAME,
        STOP_LAT_COLUMN_NAME,
        STOP_LON_COLUMN_NAME,
    );

    if let Err(e) = stops_result {
        eprintln!(
            "\nAn error occurred during processing of '{}': {}",
            STOPS_FILENAME, e
        );
        // Attempt to clean up temporary file if it exists
        let temp_output_path = gtfs_dir_path.join(format!("{}{}", STOPS_FILENAME, TEMP_SUFFIX));
        if temp_output_path.exists() {
            if let Err(remove_err) = remove_file(&temp_output_path) {
                eprintln!(
                    "Additionally, failed to remove temporary file '{}': {}",
                    temp_output_path.display(),
                    remove_err
                );
            } else {
                eprintln!("Removed temporary file '{}'.", temp_output_path.display());
            }
        }
        // Decide whether to exit or continue with shapes.txt
        // For now, let's exit on error for stops.txt as it's often crucial.
        std::process::exit(1);
    }

    // --- Execute Processing for shapes.txt ---
    let shapes_result = process_gtfs_file(
        &gtfs_dir_path,
        SHAPES_FILENAME,
        SHAPE_LAT_COLUMN_NAME,
        SHAPE_LON_COLUMN_NAME,
    );

    if let Err(e) = shapes_result {
        eprintln!(
            "\nAn error occurred during processing of '{}': {}",
            SHAPES_FILENAME, e
        );
        // Attempt to clean up temporary file if it exists
        let temp_output_path = gtfs_dir_path.join(format!("{}{}", SHAPES_FILENAME, TEMP_SUFFIX));
        if temp_output_path.exists() {
            if let Err(remove_err) = remove_file(&temp_output_path) {
                eprintln!(
                    "Additionally, failed to remove temporary file '{}': {}",
                    temp_output_path.display(),
                    remove_err
                );
            } else {
                eprintln!("Removed temporary file '{}'.", temp_output_path.display());
            }
        }
        std::process::exit(1); // Exit on error for shapes.txt as well
    }

    println!("\nProcessing complete for all files.");
}
