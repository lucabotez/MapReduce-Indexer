Copyright @lucabotez

# MapReduce Indexer

## Overview
This project implements a **parallel inverted index** using the **Map-Reduce paradigm** in **Pthreads**. The program efficiently processes multiple input files and builds an **inverted index**, mapping each unique word to the files in which it appears.

## Features
- **Parallelized word extraction** using multiple threads.
- **Mutex-based synchronization** to avoid race conditions.
- **Barrier synchronization** to ensure correct execution order.
- **Dynamic task distribution** for both **Map** and **Reduce** phases.
- **Sorted word output** by frequency and alphabetically.

## Implementation Details
### **1. Input Data Processing**
- Reads the input file containing the list of files to process.
- Creates a **queue of file-index pairs**, allowing efficient distribution of work across threads.

### **2. Map Function**
- Each thread extracts words from a file, removing duplicates.
- Words are formatted and added to the **global word list**.
- **Mutexes** are used:
  - To ensure thread-safe access when retrieving files from the queue.
  - To synchronize writing words into the global list.
- Different mutexes are used for **file queue access** and **word list updates** to improve concurrency.
- A **barrier** is used to synchronize the **completion of all Mapper threads** before proceeding to Reduce.

### **3. Reduce Function**
- Groups words by their first letter and distributes tasks dynamically using a **queue of alphabet letters**.
- Each thread processes words starting with a specific letter:
  - Extracts relevant words from the global word list.
  - Sorts words **by frequency** (descending) and **alphabetically**.
  - Writes the sorted words into the corresponding letter file.
- **Mutexes** are used to safely extract letters from the queue.
- Uses the same **barrier** to ensure that Reduce starts only after all Mapper threads finish.

### **4. Other Aspects**
- **Thread creation, mutex initialization, and barrier setup** are handled in `main`.
- **Error handling and input processing** are managed through separate functions.

## Notes
- The **Map-Reduce approach ensures efficient parallel execution**.
- **Mutex-based locking prevents data races** while allowing concurrency.
- **Sorting logic in Reduce improves readability and search efficiency**.
