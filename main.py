import pandas as pd
from metadata import Metadata
from quasi_identifier import QuasiIdentifier
from ola_algo_ri import OLA

def main():
    '''
    # Example metadata and dataset
    metadata = {"columns": ["age", "income", "height", "weight"]}
    data = pd.DataFrame({
        "age": [25, 35, 45, 55, 65,25, 35, 45, 55, 65,25, 35, 45, 55, 65],
        "income": [2000, 3000, 4000, 5000, 6000,2000, 3000, 4000, 5000, 6000,2000, 3000, 4000, 5000, 6000],
        "height": [150, 160, 170, 180, 190,150, 160, 170, 180, 190,150, 160, 170, 180, 190],
        "weight": [50, 60, 70, 80, 90,50, 60, 70, 80, 90,50, 60, 70, 80, 90]
    })
    '''

    data = pd.read_csv('KanonMedicalData.csv')
    metadata = {"columns": list(data.columns)}
    # Step 1: Initialize metadata
    meta_handler = Metadata(metadata, data)
    numerical_columns = meta_handler.get_numerical_columns()

    # Choose Q quasi-identifiers (example: first two numerical columns)
    chosen_columns = numerical_columns[1:4]
    print("Chosen columns: ", chosen_columns)
    quasi_identifiers = meta_handler.get_quasi_identifiers(chosen_columns)

    # Step 2: Initialize QuasiIdentifier objects
    quasi_identifier_objects = [
        QuasiIdentifier(data, col) for col in quasi_identifiers
    ]

    # Step 3: Run initial OLA algorithm to find Ri values
    max_equivalence_classes = 1000000 # Example max equivalence classes
    k = 2000  # Example K-anonymity value
    ola = OLA(quasi_identifier_objects, max_equivalence_classes, doubling_step=2)
    ola.build_tree()
    initial_ri = ola.find_smallest_passing_ri()

    print("Initial smallest passing bin widths (Ri):", initial_ri)
    #.ola.print_tree()
    ola.print_marked_tree()

    # Step 4: Process data in chunks to create histograms
    chunk_size = 100000  # Example chunk size for processing
    histograms = []
    for i in range(0, len(data), chunk_size):
        chunk = data.iloc[i:i + chunk_size]
        histograms.append(ola.process_chunk(chunk, initial_ri))

    global_histogram = ola.merge_histograms(histograms)

    # Step 5: Check K-anonymity
    if ola.check_k_anonymity(global_histogram, k):
        print("Initial Ri satisfies K-anonymity:", initial_ri)

        # Generalize data in chunks using the initial Ri values
        generalized_chunks = [
            ola.generalize_chunk(data.iloc[i:i + chunk_size], initial_ri)
            for i in range(0, len(data), chunk_size)
        ]
    else:
        print("Initial Ri fails K-anonymity. Re-running OLA with histogram.")

        # Re-run OLA with histogram-based K-anonymity checks
        final_ri = ola.re_run_ola_with_histogram(initial_ri, global_histogram, k)

        print("Final smallest passing bin widths (Ri):", final_ri)

        # Generalize data in chunks using the final Ri values
        generalized_chunks = [
            ola.generalize_chunk(data.iloc[i:i + chunk_size], final_ri)
            for i in range(0, len(data), chunk_size)
        ]

    # Step 6: Output generalized data chunks
    print("Generalized Data Chunks:")
    for chunk in generalized_chunks:
        print(chunk)

    final_generalized_df = ola.combine_generalized_chunks_to_csv(
    generalized_chunks, 
    output_path='generalized_medical_data.csv')

if __name__ == "__main__":
    main()
