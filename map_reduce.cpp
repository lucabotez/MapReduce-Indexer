// Copyright @lucabotez

#include <stdlib.h>
#include <pthread.h>
#include <bits/stdc++.h>

// structure use for sending data to the map function
// called by the mapper threads
struct shared_map_data {
    // all the input files received
    std::queue<std::pair<std::string, int>> *files;

    // the final list with all the words from all the files
    std::map<std::string, std::vector<int>> *word_list;

    // mutexes and barrier used for synchronizing
    pthread_mutex_t *read_mutex;
    pthread_mutex_t *write_mutex;
    pthread_barrier_t *barrier;
};

// structure use for sending data to the receive function
// called by the receiver threads
struct shared_reduce_data {
    // the final list with all the words from all the files
    std::map<std::string, std::vector<int>> *word_list;

    // a queue that contains all the lowercase letters from
    // the english alphabet
    std::queue<char> *alphabet;

    // mutexes used for synchronizing
    pthread_mutex_t *read_mutex;
    pthread_barrier_t *barrier;
};

// function used to process the input
void process_input(int argc, char **argv, int &mappers,
 int &reducers, std::queue<std::pair<std::string, int>> &files) {
    // checks the number of arguments given
    if (argc < 4) {
        std::cout << "Input error.\n";
        exit(-1);
    }

    mappers = atoi(argv[1]);
    reducers = atoi(argv[2]);

    std::string input_file;
    input_file = argv[3];

    std::ifstream fin(input_file);
    int file_number;
    fin >> file_number;

    for (int i = 1; i <= file_number; i++) {
        std::string temp_file;
        fin >> temp_file;

        // pushing both the filename and the file number
        files.push(std::make_pair(temp_file, i));
    }

    fin.close();
}

// function used to handle possible thread and sync mechanisms
// errors
void handle_error(int err, std::string type) {
    if (err) {
        if (type == "thread_create")
            printf("Error while creating thread.\n");

        if (type == "thread_join")
            printf("Error while waiting for thread.\n");

        if (type == "mutex")
            printf("Error while creating mutex.\n");

        if (type == "barrier")
            printf("Error while creating barrier.\n");

        exit(-1);
    }
}

// function used for eliminating all non-letter characters from
// a string read from the input files (",", "-", ...) and
// transforming all uppercase letters into lowercase ones
void format_word(std::string &word) {
    std::string new_word;

    // check if the character is valid or not, if it is
    // convert it to lowercase and save it in the new word
    for (size_t i = 0; i < word.size(); i++)
        if (isalpha(word[i]))
            new_word.push_back(tolower(word[i]));

    // copy the newly made word at the old one's adress
    word = std::move(new_word);
}

// function given as parameter for the mapper threads, deals with
// extracting all the words from the given input files and putting
// them in a single list implemented by a map with the keys represented
// as strings (the words themselves) and the values as vectors of ints
// (the file indexes in which the words appear)
void *map(void *arg) {
    // receiving the data
    shared_map_data* shared_data = static_cast<shared_map_data*>(arg);

    std::queue<std::pair<std::string, int>>* files = shared_data->files;
    std::map<std::string, std::vector<int>>* word_list = shared_data->word_list;

    pthread_mutex_t* read_mutex = shared_data->read_mutex;
    pthread_mutex_t* write_mutex = shared_data->write_mutex;
    pthread_barrier_t* barrier = shared_data->barrier;

    // the workload is dynamically split between all the threads; when a thread
    // finishes processing the current file it takes a new one from the files queue
    // if possible
    while (1) {
        // making sure that memory related operations are correctly
        // synchronized
        pthread_mutex_lock(read_mutex);

        if (files->empty()) {
            pthread_mutex_unlock(read_mutex);

            // signals the rest of the threads (especially
            // the reducers) that its execution is finished
            pthread_barrier_wait(barrier);
            pthread_exit(NULL);
        }

        // taking a file from the queue and removing it
        auto file = files->front();
        files->pop();
        pthread_mutex_unlock(read_mutex);

        // opening the input file and saving its index
        std::ifstream thread_fin(file.first);
        int thread_file_index = file.second;

        // by firstly putting the words in a local map
        // we make sure that there are no duplicates,
        // making it faster to update the agregated list
        // and spending less time in a mutex lock
        std::map<std::string, int> current_word_list;
        std::string word;

        while (thread_fin >> word) {
            format_word(word);
            current_word_list.insert(std::make_pair(word, thread_file_index));
        }

        thread_fin.close();

        // updating the word list accordingly
        pthread_mutex_lock(write_mutex);

        for (const auto& pair : current_word_list) {
            std::string aux_word = pair.first;
            int index = pair.second;

            // if a word already is in the list, only add the
            // current file index to the vector of indexes
            auto iterator = word_list->find(aux_word);
            if (iterator == word_list->end())
                (*word_list)[aux_word] = {index};
            else
                (*word_list)[aux_word].push_back(index);
        }

        pthread_mutex_unlock(write_mutex);
    }
}

// function given as parameter for the reducer threads, extracts the words
// from the agregated list letter by letter, sorts them accordingly and 
// writes them in the output files for their respective letter
void *reduce(void *arg) {
    // receiving the data
    shared_reduce_data* shared_data = static_cast<shared_reduce_data*>(arg);

    std::map<std::string, std::vector<int>>* word_list = shared_data->word_list;
    std::queue<char>* alphabet = shared_data->alphabet;

    pthread_mutex_t* read_mutex = shared_data->read_mutex;
    pthread_barrier_t* barrier = shared_data->barrier;

    // making sure that all mapper threads finished execution
    pthread_barrier_wait(barrier);

    // similarly to the map function logic, the workload is dynamically split;
    // each thread processes a letter, when it finishes takes another one from
    // the alphabet queue
    while (1) {
        // making sure that memory related operations are correctly
        // synchronized
        pthread_mutex_lock(read_mutex);

        if (!alphabet->empty()) {
            char file_letter = alphabet->front();
            alphabet->pop();
            pthread_mutex_unlock(read_mutex);

            // creating the corresponding output file
            std::ofstream thread_fout(std::string(1, file_letter) + ".txt");

            // extracting only the key-value pairs from the map with the key
            // starting with the required letter
            std::vector<std::pair<std::string, std::vector<int>>> letter_pairs;
            for (const auto& pair : (*word_list))
                if (!pair.first.empty() && pair.first[0] == file_letter)
                    letter_pairs.push_back(pair);

            // sorting the pairs descending by the number of indexes and
            // in alphabetical order
            std::sort(letter_pairs.begin(), letter_pairs.end(),
              [](const std::pair<std::string, std::vector<int>>& a,
                 const std::pair<std::string, std::vector<int>>& b) {
                  // comparing the index vector size
                  if (a.second.size() != b.second.size()) {
                      return a.second.size() > b.second.size();
                  }
                  
                  // if the sizes are equal, sort them alphabetically
                  return a.first < b.first;
              });

            // printing the pairs in the output file
            for (auto& pair : letter_pairs) {
                thread_fout << pair.first << ":[";

                // sorting the indexes in ascending order
                std::sort(pair.second.begin(), pair.second.end());
                
                size_t i = 0;
                for (; i < pair.second.size() - 1; i++)
                    thread_fout << pair.second[i] << " ";

                thread_fout << pair.second[i];
                thread_fout << "]" << std::endl;
            }

            thread_fout.close();
        } else {
            pthread_mutex_unlock(read_mutex);
            pthread_exit(NULL);
        }
    }
}

int main(int argc, char **argv)
{
    // number of mapper and reducer threads
    int mappers, reducers;

    // the filenames are saved in a queue of pairs, in
    // which the second element is the index of the file
    std::queue<std::pair<std::string, int>> files;

    process_input(argc, argv, mappers, reducers, files);

    int err; // error code
    pthread_mutex_t read_mutex, write_mutex;
    pthread_barrier_t barrier;

    // initializing the mutexes
    err = pthread_mutex_init(&read_mutex, NULL);
	handle_error(err, "mutex");

    err = pthread_mutex_init(&write_mutex, NULL);
	handle_error(err, "mutex");
    
    // initializing the barrier
    err = pthread_barrier_init(&barrier, NULL, mappers + reducers);
	handle_error(err, "barrier");

    // initializing the word list
    std::map<std::string, std::vector<int>> word_list;

    // creating the alphabet used in the reduce phase
    std::queue<char> alphabet;
    for (char c = 'a'; c <= 'z'; c++)
        alphabet.push(c);

    /// preparing mapper thread data
    shared_map_data shared_map_data = {
        &files,
        &word_list,
        &read_mutex,
        &write_mutex,
        &barrier
    };

    /// preparing reducer thread data
    shared_reduce_data shared_reduce_data = {
        &word_list,
        &alphabet,
        &read_mutex,
        &barrier
    };

    // creating all the required threads
    pthread_t* threads = (pthread_t*)malloc((mappers + reducers)
                       * sizeof(pthread_t));
    for (int i = 0; i < mappers + reducers; i++) {
        // making sure that each thread calls the correct function
        if (i < mappers)
            err = pthread_create(&threads[i], NULL, map, &shared_map_data);
        else
            err = pthread_create(&threads[i], NULL, reduce, &shared_reduce_data);

       	handle_error(err, "thread_create");
    }

    // joining all the threads
    for (int i = 0; i < mappers + reducers; i++) {
		err = pthread_join(threads[i], NULL);
		handle_error(err, "thread_join");
	}

    // clearing everything
    free(threads);
    pthread_barrier_destroy(&barrier);
    pthread_mutex_destroy(&read_mutex);
    pthread_mutex_destroy(&write_mutex);

    return 0;
}
