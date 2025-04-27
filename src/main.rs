use std::env;
use std::error::Error;
use std::fs::{File, rename};
use std::io::{BufReader, BufWriter, Write}; // Added Write for flushing stdout
use std::path::{Path, PathBuf};
use csv::{ReaderBuilder, Writer, StringRecord};

// --- Configuration ---
// Name of the stops file within the GTFS directory
const STOPS_FILENAME: &str = "stops.txt";
// Temporary file suffix
const TEMP_SUFFIX: &str = ".tmp";
// 0-based index of the latitude column (Standard GTFS)
const LAT_COLUMN_INDEX: usize = 4;
// 0-based index of the longitude column (Standard GTFS)
const LON_COLUMN_INDEX: usize = 5;
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

/// Reads the stops.txt file from the specified GTFS directory,
/// fixes coordinate formats, and overwrites the original file.
/// Uses a temporary file to ensure atomicity.
///
/// # Arguments
/// * `gtfs_dir` - Path to the directory containing the GTFS files.
///
/// # Returns
/// `Ok(())` on success, or an `Err` containing the error information.
fn process_stops_file(gtfs_dir: &Path) -> Result<(), Box<dyn Error>> {
    // Construct full paths for input and temporary output files
    let input_path = gtfs_dir.join(STOPS_FILENAME);
    let temp_output_filename = format!("{}{}", STOPS_FILENAME, TEMP_SUFFIX);
    let temp_output_path = gtfs_dir.join(&temp_output_filename);

    println!("Starting processing of '{}'...", input_path.display());

    // --- Input File Handling ---
    if !input_path.exists() {
        eprintln!("Error: Input file '{}' not found in directory '{}'.", STOPS_FILENAME, gtfs_dir.display());
        return Err(format!("Input file not found: {}", input_path.display()).into());
    }
    let input_file = File::open(&input_path)?;
    let reader = BufReader::new(input_file);

    // Configure CSV reader
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true) // Assume the first row is a header
        .from_reader(reader);

    // --- Temporary Output File Handling ---
    // Create the temporary file for writing
    let temp_output_file = File::create(&temp_output_path)?;
    let writer = BufWriter::new(temp_output_file);
    let mut csv_writer = Writer::from_writer(writer);

    // --- Header Processing ---
    let headers = csv_reader.headers()?.clone(); // Clone to own the data

    // Validate column indices
    if headers.len() <= LAT_COLUMN_INDEX || headers.len() <= LON_COLUMN_INDEX {
         eprintln!(
            "Error: CSV file '{}' has fewer columns ({}) than expected for lat ({}) or lon ({}).",
            input_path.display(),
            headers.len(),
            LAT_COLUMN_INDEX + 1, // Display 1-based index to user
            LON_COLUMN_INDEX + 1
        );
        // Clean up the temporary file before erroring
        std::fs::remove_file(&temp_output_path)?;
        return Err("Insufficient columns in CSV header.".into());
    }
     println!(
        "Identified Latitude column: '{}' (Index {})",
        headers.get(LAT_COLUMN_INDEX).unwrap_or("N/A"),
        LAT_COLUMN_INDEX
    );
     println!(
        "Identified Longitude column: '{}' (Index {})",
        headers.get(LON_COLUMN_INDEX).unwrap_or("N/A"),
        LON_COLUMN_INDEX
    );

    // Write the header to the temporary output file
    csv_writer.write_record(&headers)?;
    println!("Header written to temporary file '{}'.", temp_output_path.display());

    // --- Record Processing ---
    let mut processed_count = 0;
    let mut record = StringRecord::new(); // Reusable record

    // Iterate over each record in the input file
    while csv_reader.read_record(&mut record)? {
        let mut output_fields: Vec<String> = Vec::with_capacity(record.len());

        // Process each field, formatting coordinates as needed
        for (index, field) in record.iter().enumerate() {
            let processed_field = if index == LAT_COLUMN_INDEX || index == LON_COLUMN_INDEX {
                format_coordinate(field)
            } else {
                field.to_string()
            };
            output_fields.push(processed_field);
        }

        // Write the potentially modified record to the temporary output CSV
        csv_writer.write_record(&output_fields)?;
        processed_count += 1;

        // Optional: Progress indicator
        if processed_count % 1000 == 0 {
             print!("\rProcessed {} records...", processed_count);
             stdout().flush()?; // Ensure the progress message is displayed immediately
        }
    }
    println!("\rProcessed {} records.      ", processed_count); // Clear progress line

    // --- Finalisation ---
    // Ensure all buffered data is written to the temporary file
    csv_writer.flush()?;
    println!("Successfully processed {} records to temporary file.", processed_count);

    // --- Replace Original File ---
    // Rename the temporary file to the original filename, overwriting it.
    // This is generally an atomic operation on most filesystems.
    rename(&temp_output_path, &input_path)?;
    println!("Successfully replaced '{}' with the processed data.", input_path.display());

    Ok(())
}

// Helper function to get stdout handle easily
fn stdout() -> std::io::Stdout {
    std::io::stdout()
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
        eprintln!("Error: Provided path '{}' is not a valid directory.", gtfs_dir_path.display());
        std::process::exit(1);
    }

    // --- Execute Processing ---
    if let Err(e) = process_stops_file(&gtfs_dir_path) {
        eprintln!("\nAn error occurred during processing: {}", e);
        // Attempt to clean up temporary file if it exists on error
        let temp_output_filename = format!("{}{}", STOPS_FILENAME, TEMP_SUFFIX);
        let temp_output_path = gtfs_dir_path.join(&temp_output_filename);
        if temp_output_path.exists() {
            if let Err(remove_err) = std::fs::remove_file(&temp_output_path) {
                 eprintln!("Additionally, failed to remove temporary file '{}': {}", temp_output_path.display(), remove_err);
            } else {
                 eprintln!("Removed temporary file '{}'.", temp_output_path.display());
            }
        }
        std::process::exit(1); // Exit with a non-zero code
    }

    println!("Processing complete.");
}

