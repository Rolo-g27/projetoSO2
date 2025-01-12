#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdio.h>
#include <pthread.h>
#include <sys/stat.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h> 
#include <time.h>
#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "io.h"
#include <sys/types.h>



// ---------------------------------------------------
// ESTRUTURAS DE DADOS
// ---------------------------------------------------

struct SharedData {
    DIR* dir;
    char* dir_name;
    pthread_mutex_t directory_mutex;
};

// Informação de conexão de um cliente.
// Ajuste se quiser guardar mais/menos informações.


// ---------------------------------------------------
// VARIÁVEIS GLOBAIS
// ---------------------------------------------------
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0;     // Number of active backups
size_t max_backups;            // Maximum allowed simultaneous backups
size_t max_threads;            // Maximum allowed simultaneous threads
char* jobs_directory = NULL;
char* fifo_path = NULL;        // Path to the FIFO for registration
int fifo_fd = -1;              // File descriptor for the FIFO

// ---------------------------------------------------
// LIMPEZA DO FIFO AO ENCERRAR
// ---------------------------------------------------
void cleanup_fifo() {
    if (fifo_fd != -1) {
        close(fifo_fd);
    }
    if (fifo_path != NULL) {
        printf("Removendo FIFO no encerramento: %s\n", fifo_path);
        unlink(fifo_path);
    }
}

// ---------------------------------------------------
// FUNÇÕES AUXILIARES EXISTENTES
// ---------------------------------------------------

int filter_job_files(const struct dirent* entry) {
    const char* dot = strrchr(entry->d_name, '.');
    if (dot != NULL && strcmp(dot, ".job") == 0) {
        return 1;  // Keep this file (it has the .job extension)
    }
    return 0;
}

static int entry_files(const char* dir, struct dirent* entry, char* in_path, char* out_path) {
    const char* dot = strrchr(entry->d_name, '.');
    if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 || strcmp(dot, ".job")) {
        return 1;
    }

    if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
        fprintf(stderr, "%s/%s\n", dir, entry->d_name);
        return 1;
    }

    strcpy(in_path, dir);
    strcat(in_path, "/");
    strcat(in_path, entry->d_name);

    strcpy(out_path, in_path);
    strcpy(strrchr(out_path, '.'), ".out");

    return 0;
}

static int run_job(int in_fd, int out_fd, char* filename) {
    size_t file_backups = 0;
    while (1) {
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        unsigned int delay;
        size_t num_pairs;

        switch (get_next(in_fd)) {
            case CMD_WRITE: {
                num_pairs = parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_write(num_pairs, keys, values)) {
                    write_str(STDERR_FILENO, "Failed to write pair\n");
                }
                break;
            }
            case CMD_READ: {
                num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_read(num_pairs, keys, out_fd)) {
                    write_str(STDERR_FILENO, "Failed to read pair\n");
                }
                break;
            }
            case CMD_DELETE: {
                num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_delete(num_pairs, keys, out_fd)) {
                    write_str(STDERR_FILENO, "Failed to delete pair\n");
                }
                break;
            }
            case CMD_SHOW: {
                kvs_show(out_fd);
                break;
            }
            case CMD_WAIT: {
                if (parse_wait(in_fd, &delay, NULL) == -1) {
                    write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (delay > 0) {
                    printf("Waiting %d seconds\n", delay / 1000);
                    kvs_wait(delay);
                }
                break;
            }
            case CMD_BACKUP: {
                pthread_mutex_lock(&n_current_backups_lock);
                if (active_backups >= max_backups) {
                    wait(NULL);
                } else {
                    active_backups++;
                }
                pthread_mutex_unlock(&n_current_backups_lock);
                int aux = kvs_backup(++file_backups, filename, jobs_directory);
                if (aux < 0) {
                    write_str(STDERR_FILENO, "Failed to do backup\n");
                } else if (aux == 1) {
                    return 1;
                }
                break;
            }
            case CMD_INVALID: {
                write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
                break;
            }
            case CMD_HELP: {
                write_str(STDOUT_FILENO,
                    "Available commands:\n"
                    "  WRITE [(key,value)(key2,value2),...]\n"
                    "  READ [key,key2,...]\n"
                    "  DELETE [key,key2,...]\n"
                    "  SHOW\n"
                    "  WAIT <delay_ms>\n"
                    "  BACKUP\n" // Not implemented
                    "  HELP\n");
                break;
            }
            case CMD_EMPTY: {
                break;
            }
            case EOC: {
                printf("EOF\n");
                return 0;
            }
        }
    }
}

// Thread que processa os .job files
static void* get_file(void* arguments) {
    struct SharedData* thread_data = (struct SharedData*) arguments;
    DIR* dir = thread_data->dir;
    char* dir_name = thread_data->dir_name;

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
        fprintf(stderr, "Thread failed to lock directory_mutex\n");
        return NULL;
    }

    struct dirent* entry;
    char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
    while ((entry = readdir(dir)) != NULL) {
        if (entry_files(dir_name, entry, in_path, out_path)) {
            continue;
        }

        if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
            fprintf(stderr, "Thread failed to unlock directory_mutex\n");
            return NULL;
        }

        int in_fd = open(in_path, O_RDONLY);
        if (in_fd == -1) {
            write_str(STDERR_FILENO, "Failed to open input file: ");
            write_str(STDERR_FILENO, in_path);
            write_str(STDERR_FILENO, "\n");
            pthread_exit(NULL);
        }

        int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (out_fd == -1) {
            write_str(STDERR_FILENO, "Failed to open output file: ");
            write_str(STDERR_FILENO, out_path);
            write_str(STDERR_FILENO, "\n");
            pthread_exit(NULL);
        }

        int out = run_job(in_fd, out_fd, entry->d_name);

        close(in_fd);
        close(out_fd);

        if (out) {
            if (closedir(dir) == -1) {
                fprintf(stderr, "Failed to close directory\n");
                return 0;
            }
            exit(0);
        }

        if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
            fprintf(stderr, "Thread failed to lock directory_mutex\n");
            return NULL;
        }
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
        fprintf(stderr, "Thread failed to unlock directory_mutex\n");
        return NULL;
    }

    pthread_exit(NULL);
}

// Cria várias threads para processar os .job files
static void dispatch_threads(DIR* dir) {
    pthread_t* threads = malloc(max_threads * sizeof(pthread_t));
    if (threads == NULL) {
        fprintf(stderr, "Failed to allocate memory for threads\n");
        return;
    }

    struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};

    for (size_t i = 0; i < max_threads; i++) {
        if (pthread_create(&threads[i], NULL, get_file, (void*)&thread_data) != 0) {
            fprintf(stderr, "Failed to create thread %zu\n", i);
            pthread_mutex_destroy(&thread_data.directory_mutex);
            free(threads);
            return;
        }
    }

    for (unsigned int i = 0; i < max_threads; i++) {
        if (pthread_join(threads[i], NULL) != 0) {
            fprintf(stderr, "Failed to join thread %u\n", i);
            pthread_mutex_destroy(&thread_data.directory_mutex);
            free(threads);
            return;
        }
    }

    if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
        fprintf(stderr, "Failed to destroy directory_mutex\n");
    }

    free(threads);
}

// ---------------------------------------------------
// FUNÇÕES NOVAS PARA CONEXÃO COM O CLIENTE
// ---------------------------------------------------

static void* session_thread_func(void* arg) {
    Session* s = (Session*)arg;

    char buffer[128]; // Buffer para pedidos do cliente
    while (1) {
        ssize_t bytes_read = read(s->fd_requests, buffer, sizeof(buffer) - 1);
        if (bytes_read > 0) {
            buffer[bytes_read] = '\0'; // Garante que a string esteja terminada
            printf("Received request: %s\n", buffer);

            // Processa o comando DISCONNECT
            if (strcmp(buffer, "DISCONNECT\n") == 0) {
                printf("Client requested disconnect\n");
                break;  // Sai do loop ao receber DISCONNECT
            } 
            // Processa o comando SUBSCRIBE
            else if (strncmp(buffer, "SUBSCRIBE", 9) == 0) {
                char key[MAX_KEY_LENGTH];
                if (sscanf(buffer + 10, "%s", key) == 1) {
                    printf("Inscrevendo cliente na chave %s\n", key);
                    subscribe_client(s, key); // Função para inscrever o cliente
                    const char* response = "SUBSCRIBED\n";
                    if (write(s->fd_responses, response, strlen(response)) < 0) {
                        perror("Erro ao enviar resposta SUBSCRIBED");
                    }
                } else {
                    const char* error = "ERRO: Comando SUBSCRIBE malformado\n";
                    if (write(s->fd_responses, error, strlen(error)) < 0) {
                        perror("Erro ao enviar erro do comando SUBSCRIBE");
                    }
                }
            } 
            // Processa o comando PUBLISH
            else if (strncmp(buffer, "PUBLISH", 7) == 0) {
                char key[MAX_KEY_LENGTH], message[MAX_MESSAGE_LENGTH];
                if (sscanf(buffer + 8, "%s %[^\n]", key, message) == 2) {
                    printf("Publicando mensagem na chave %s: %s\n", key, message);
                    publish_message(key, message, s->fd_responses); // Passa o descritor do cliente que publicou
                    const char* response = "MESSAGE PUBLISHED\n";
                    if (write(s->fd_responses, response, strlen(response)) < 0) {
                        perror("Erro ao enviar resposta MESSAGE PUBLISHED");
                    }
                } else {
                    const char* error = "ERRO: Comando PUBLISH malformado\n";
                    if (write(s->fd_responses, error, strlen(error)) < 0) {
                        perror("Erro ao enviar erro do comando PUBLISH");
                    }
                }
            }

            // Processa o comando UNSUBSCRIBE
            else if (strncmp(buffer, "UNSUBSCRIBE", 11) == 0) {
                char key[MAX_KEY_LENGTH];
                if (sscanf(buffer + 12, "%s", key) == 1) {
                    printf("Cancelando inscrição do cliente na chave %s\n", key);
                    unsubscribe_client(s, key); // Função para remover inscrição
                    const char* response = "UNSUBSCRIBED\n";
                    if (write(s->fd_responses, response, strlen(response)) < 0) {
                        perror("Erro ao enviar resposta UNSUBSCRIBED");
                    }
                } else {
                    const char* error = "ERRO: Comando UNSUBSCRIBE malformado\n";
                    if (write(s->fd_responses, error, strlen(error)) < 0) {
                        perror("Erro ao enviar erro do comando UNSUBSCRIBE");
                    }
                }
            } 
            // Comando desconhecido
            else {
                const char* unknown = "UNKNOWN COMMAND\n";
                if (write(s->fd_responses, unknown, strlen(unknown)) < 0) {
                    perror("Erro ao enviar comando desconhecido");
                }
            }
        } else if (bytes_read == 0) {
            // FIFO fechado pelo cliente (EOF)
            printf("FIFO fechado pelo cliente. Finalizando sessão.\n");
            break;  // Sai do loop ao detectar EOF
        } else {
            perror("Erro ao ler pedido do cliente");
            break;  // Sai do loop em caso de erro
        }
    }

    // Limpeza da sessão
    printf("Limpando sessão do cliente...\n");
    close(s->fd_requests);
    close(s->fd_responses);

    // Remove os FIFOs do cliente, verificando antes se eles ainda existem
    if (access(s->fifo_requests, F_OK) == 0) {
        if (unlink(s->fifo_requests) == 0) {
            printf("FIFO de pedidos removido: %s\n", s->fifo_requests);
        } else {
            perror("Erro ao remover FIFO de pedidos");
        }
    } else {
        printf("FIFO de pedidos já removido: %s\n", s->fifo_requests);
    }

    if (access(s->fifo_responses, F_OK) == 0) {
        if (unlink(s->fifo_responses) == 0) {
            printf("FIFO de respostas removido: %s\n", s->fifo_responses);
        } else {
            perror("Erro ao remover FIFO de respostas");
        }
    } else {
        printf("FIFO de respostas já removido: %s\n", s->fifo_responses);
    }

    // Remove inscrições ativas do cliente
    unsubscribe_all(s);

    free(s);
    return NULL;
}





static void accept_connections() {
    char buffer[512];
    int idle_counter = 0; // Contador para limitar logs de "FIFO vazio"

    while (1) {
        ssize_t count = read(fifo_fd, buffer, sizeof(buffer) - 1);
        if (count > 0) {
            buffer[count] = '\0'; // Garante que o buffer seja uma string válida

            // Divide o buffer em múltiplas linhas, caso tenha mais de uma
            char* line = strtok(buffer, "\n");
            while (line != NULL) {
                printf("Mensagem recebida: %s\n", line);

                char fifo_req[PATH_MAX];
                char fifo_res[PATH_MAX];

                // Parse da linha para separar fifo_req e fifo_res
                if (sscanf(line, "%[^;];%s", fifo_req, fifo_res) == 2) {
                    printf("FIFO de pedidos: %s, FIFO de respostas: %s\n", fifo_req, fifo_res);
                    printf("Verificando existência dos FIFOs...\n");

                    // Verifica se os FIFOs existem
                    if (access(fifo_req, F_OK) == -1 || access(fifo_res, F_OK) == -1) {
                        fprintf(stderr, "FIFO de cliente não encontrado.\n");
                        line = strtok(NULL, "\n");
                        continue;
                    }

                    // Cria sessão
                    Session* s = calloc(1, sizeof(Session));
                    if (!s) {
                        perror("Erro ao alocar sessão");
                        line = strtok(NULL, "\n");
                        continue;
                    }

                    s->client_pid = getpid();
                    strncpy(s->fifo_requests, fifo_req, PATH_MAX);
                    strncpy(s->fifo_responses, fifo_res, PATH_MAX);

                    s->fd_requests = open(fifo_req, O_RDWR); // O_RDWR evita EOF prematuro
                    s->fd_responses = open(fifo_res, O_RDWR); // O_RDWR evita EOF prematuro

                    if (s->fd_requests == -1 || s->fd_responses == -1) {
                        perror("Erro ao abrir FIFOs do cliente");
                        if (s->fd_requests != -1) close(s->fd_requests);
                        if (s->fd_responses != -1) close(s->fd_responses);
                        free(s);
                        line = strtok(NULL, "\n");
                        continue;
                    }

                    if (pthread_create(&s->tid, NULL, session_thread_func, s) != 0) {
                        perror("Erro ao criar thread");
                        close(s->fd_requests);
                        close(s->fd_responses);
                        free(s);
                        line = strtok(NULL, "\n");
                        continue;
                    }

                    pthread_detach(s->tid);
                } else {
                    fprintf(stderr, "Mensagem malformada: %s\n", line);
                }

                // Passa para a próxima linha no buffer
                line = strtok(NULL, "\n");
            }
            idle_counter = 0; // Reset no contador ao processar algo
        } else if (count == 0) {
            // EOF no FIFO
            if (idle_counter % 5 == 0) { // Exibe log a cada 5 ciclos inativos
                static int empty_count = 0;
                if (++empty_count % 5 == 0) { // Exibe a cada 5 iterações
                    printf("FIFO de registo vazio. Aguardando novos clientes...\n");
                }
            }
            idle_counter++;
            struct timespec ts = {0, 500000000}; // 500 ms
            nanosleep(&ts, NULL);
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // Nenhuma mensagem no FIFO no momento
            struct timespec ts = {0, 500000000}; // 500 ms
            nanosleep(&ts, NULL);
        } else {
            // Erro inesperado
            perror("Erro ao ler FIFO de registo");
            break;
        }
    }
}



int main(int argc, char** argv) {
    if (argc < 5) {
        write_str(STDERR_FILENO, "Usage: ");
        write_str(STDERR_FILENO, argv[0]);
        write_str(STDERR_FILENO, " <jobs_dir>");
        write_str(STDERR_FILENO, " <max_threads>");
        write_str(STDERR_FILENO, " <max_backups>");
        write_str(STDERR_FILENO, " <fifo_path>\n");
        return 1;
    }

    jobs_directory = argv[1];
    fifo_path = argv[4];

    char* endptr;
    max_threads = strtoul(argv[2], &endptr, 10);
    if (*endptr != '\0') {
        fprintf(stderr, "Invalid max_threads value\n");
        return 1;
    }

    max_backups = strtoul(argv[3], &endptr, 10);
    if (*endptr != '\0') {
        fprintf(stderr, "Invalid max_backups value\n");
        return 1;
    }

    if (max_backups <= 0) {
        write_str(STDERR_FILENO, "Invalid number of backups\n");
        return 0;
    }

    if (max_threads <= 0) {
        write_str(STDERR_FILENO, "Invalid number of threads\n");
        return 0;
    }

    if (kvs_init()) {
        write_str(STDERR_FILENO, "Failed to initialize KVS\n");
        return 1;
    }

    printf("Tentando criar FIFO em: %s\n", fifo_path);
  if (mkfifo(fifo_path, 0666) == -1) {
      if (errno != EEXIST) {
          perror("Failed to create FIFO");
          exit(EXIT_FAILURE);
      } else {
          printf("FIFO já existia: %s\n", fifo_path);
      }
  } else {
      printf("FIFO criado com sucesso: %s\n", fifo_path);
  }

  printf("FIFO criado, verificando acesso: %s\n", fifo_path);
  if (access(fifo_path, F_OK) == -1) {
      perror("FIFO desapareceu antes de abrir");
      exit(EXIT_FAILURE);
  }


  // Verifique se o FIFO realmente existe após a criação
  if (access(fifo_path, F_OK) == -1) {
      perror("FIFO não encontrado após criação");
      exit(EXIT_FAILURE);
  }


    // Open the FIFO in non-blocking mode
    fifo_fd = open(fifo_path, O_RDONLY | O_NONBLOCK);
    if (fifo_fd == -1) {
    perror("Failed to open FIFO");
    exit(EXIT_FAILURE);
    }
    printf("FIFO aberto com sucesso: %s\n", fifo_path);


    // Registrar a limpeza do FIFO no exit
    atexit(cleanup_fifo);

    // Abre o diretório para processar .job files
    DIR* dir = opendir(jobs_directory);
    if (!dir) {
        perror("Failed to open jobs directory");
        return 1;
    }

    // Lança as threads que processam os .job
    // (Este passo pode ser opcional dependendo do seu design,
    // mas deixei conforme seu código original.)
    dispatch_threads(dir);

    if (closedir(dir) == -1) {
        perror("Failed to close jobs directory");
        return 0;
    }

    // Agora, ficamos aceitando conexões de clientes em paralelo.
    // Você pode:
    // 1) rodar accept_connections() aqui no main mesmo (bloqueante).
    // 2) criar uma thread para accept_connections().
    // 3) ou outro design. Abaixo é direto:

    accept_connections();

    // Quando accept_connections sair (se sair), esperamos backups pendentes
    while (active_backups > 0) {
        wait(NULL);
        active_backups--;
    }

    kvs_terminate();
    return 0;
}
