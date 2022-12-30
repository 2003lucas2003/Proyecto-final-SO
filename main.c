/*
 * PROYECTO II., curso 2022-2023. Sistemas Operativos
 * AUTOR: Miguel Ángel Mesas Uzal
 * FECHA: Diciembre de 2022
 *
 * DESCRIPCIÓN: Esqueleto cumpliendo los requisitos de la pŕactica 2 pendiente de parametrización
 * sobre los hijos CALCuladores y la verbosidad.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/types.h>
#include <sys/wait.h>

#define LONGITUD_MSG 100
#define LONGITUD_MSG_INFO_ERR 200

//Códigos de exit por error
#define ERR_GET_QUEUE_KEY  1
#define ERR_CREATE_QUEUE   2
#define ERR_SEND    3
#define ERR_RECV    4
#define ERR_FSAL    5
#define ERR_MSG_NOT_EXPECTED 6

//Rango de búsqueda
#define BASE 800000000
#define RANGO 2000
#define LIMITE (BASE+RANGO)
#define RANGO_BUSQUEDA (LIMITE-BASE)

///Códigos de mensaje para el campo msg_type del tipo T_MSG_BUFFER
#define MSG_COD_ESTOY_AQUI 2
#define MSG_COD_LIMITES 3
#define MSG_COD_RESULTADOS 4
#define MSG_COD_FIN 5

///Ficheros
#define NOMBRE_FICHERO_SALIDA "primos.txt"
#define NOMBRE_FICHERO_CUENTA_PRIMOS "cuentaprimos.txt"
#define FRECUENCIA_ESCRITURA_CUENTA_PRIMOS 5

///Frecuencia del temporizador para el proceso RAÍZ
#define INTERVALO_TIMER  5

typedef struct {
    long msg_type;
    char msg_text[LONGITUD_MSG];
} T_MSG_BUFFER;

int  comprobarSiEsPrimo(long int numero);
void imprimirJerarquiaPorcesos(int pidRaiz, int pidServidor, int *pidHijos, int numHijos);
void informar(char *texto, int verboso);
long int contarLineas();
void alarmHandler(int signo);

///Variable para el cómputo del tiempo total invertido en la búsqueda de los primos
int    computoTotalSegundos=0;
int    messageQueueId;



int main(int argc, char *argv[]) {

    //Limitar la entrada de argumentos
    if(argc!=3){
        printf("ERROR. Número de argumentos incorrecto.\n");
        return -1;
    } 

///raíz, servidor, proceso 'actual' , proceso padre, retorno fork, proceso calculador
    int   rootPid, serverPid, currentPid, parentPid, pid, pidCalculador;
    int   *pidHijos;     ///Para conocer los pid de los hijos
    int   i,j, numHijos; ///Iterar en la creación de hijos y el total de hijos.
    int   verbosity = 1; ///Marca la visibilidad por consola en cada primo encontrado

///Contenido de mensaje que se intercambia a través de la cola
    key_t keyQueue;
    T_MSG_BUFFER message;

///Registros de tiempos
    time_t startTime, endTime;

///Variables de partición del rango de búsqueda
///intervalo_hijo = RANGO_BUSQUEDA/num_hijos. Cada hijo busca en [base_hijo,limite_hijo)
    long int intervalo_hijo, base_hijo, limite_hijo;


///Contenido informativo hacia la consola
    char info[LONGITUD_MSG_INFO_ERR];


//Control de los procesos calculadores que han finalizado
    int numeroCalculadoresFinalizados=0; ///Hasta que esta variable no coincida con nunHijos el programa debe continuar
    long int numerosPrimosEncontrados=0;

//Ficheros
    FILE *ficheroSalida, *ficheroCuentaPrimos;

    numHijos = strtol((argv[1]),NULL,10);
    verbosity = strtol((argv[2]),NULL,10);


   //printf("numHijos: %d\n\n", numHijos);

    rootPid = getpid();
    pid = fork(); ///Creación del Server
    if (pid==0) { ///si pid==0 -> Server es hijo del raíz
        pid = getpid(); /// pid identifica el proceso después de la clonación padre-hijo
        serverPid = pid; /// proceso Server
        currentPid = serverPid; ///Se está ejecutando el proceso Server
        ///1.Creación de la cola de mensajería.
        ///1.1Creación de la Queue key,  key = ftok("archivo existente","identificador de proyecto!=0)
        if ((keyQueue = ftok("/tmp",'C') == -1)){
            perror("Error al crear la System V IPC key");
            exit(ERR_GET_QUEUE_KEY);
        }
        ///Visualización de la Queue key
        ///printf("Server V IPC key = %u\n",keyQueue);
        ///1.2 Creación de la cola de mensajería
        ///msgget get a System V message queue identifier
        if ((messageQueueId = msgget(keyQueue, IPC_CREAT | 0666) ) == -1) {
            perror("Fallo al crear la System V IPC queue");
            exit(ERR_CREATE_QUEUE);
        }
        ///Visualización de la Message Queue Id
        printf("Server: Message queue id =%u\n",messageQueueId);

        ///Creación de los procesos CALCuladores
        i=0;
        while(i<numHijos){
            if (pid>0){ ///SOLO Server (serverPid) creará hijos
                pid= fork();
                if (pid==0){ ///Rama hijo CALCulador
                    parentPid = getppid(); /// Soy hijo de mi padre
                    currentPid = getpid(); /// Soy un calculador
                    printf("Ha nacido un hijo calculdador %d, ordinal de hijo %d\n",currentPid,i);
                }
            }
            i++;///Número de hijos creados
        }
        ///En este punto hay dicotomia de ejecución para currentPid, serverPid y hijos calculadores
        ///Si soy un hijo calculador, tengo que decirle al Server que vivo y estoy preparado para calcular
        ///printf("Soy %d y el server es %d\n",currentPid,serverPid);
        if (currentPid!=serverPid){ ///Soy un hijo CALCulador, mi trabajo es informar que existo y calcular primos.
           /// printf("Soy un hijo calculador y voy a decirselo al server %d debo ser igual a %d\n",getpid(),currentPid);
            message.msg_type = MSG_COD_ESTOY_AQUI; ///Establecemos el tipo de mensaje
            sprintf(message.msg_text,"%d",currentPid);///Identificador del hijo CALCulador
            msgsnd(messageQueueId,&message, sizeof(message), IPC_NOWAIT);

            message.msg_type = MSG_COD_LIMITES;
            ///Se queda a la espera de recibir el mensaje de límites de operación
            ///printf("Soy el hijo %d y estoy a la espera del intervalo de búsqueda\n",currentPid);
            msgrcv(messageQueueId,&message, sizeof(message), MSG_COD_LIMITES,0);
            ///En el contenido del mensaje figuran la base y el rango o límite, msg_text
            sscanf(message.msg_text,"%ld %ld",&base_hijo,&limite_hijo);
            //printf("Child says: He recibido mi intervalo de búsqueda [%ld,%ld)\n",base_hijo,limite_hijo);

            ///Tarea 1. Búsqueda de números primos en su rango
            for (long int numero = base_hijo; numero < limite_hijo; numero++) {
                if (comprobarSiEsPrimo(numero)) {
                    ///Eureka!!, se ha encontrado un número primo por fuerza bruta
                    ///Notificamos al Server escribiendo en el mensaje el hijo que lo ha encontrado y el primo
                    message.msg_type = MSG_COD_RESULTADOS;
                    sprintf(message.msg_text, "%d %ld", currentPid, numero);
                    msgsnd(messageQueueId, &message, sizeof(message), IPC_NOWAIT);
                    ///printf("Se ha localizado un primo y se ha enviado al Server %d\n",numero);
                }
            }
            ////Tarea 2. Fin de procesamiento
            ///T_MSG_BUFFER messageEnd;
            message.msg_type = MSG_COD_FIN;
            sprintf(message.msg_text, "%d", currentPid);
            msgsnd(messageQueueId, &message, sizeof(message), IPC_NOWAIT);


        } else {///Soy el Server
            ///Crea la memoria dinámica para identificar los hijos
            pidHijos = (int*) malloc(numHijos* sizeof(int));
            printf("Hijos creados pero a la espera de enviarles trabajo\n");
            for (j=0; j<numHijos;j++){ ///Recepción de los mensajes COD_ESTOY_AQUI
                ///printf("A la espera del hijo %d-",j);
                //lectura y bloquea la ejecución hasta tener mensaje disponible
                msgrcv(messageQueueId,&message,sizeof(message),MSG_COD_ESTOY_AQUI,0);
                sscanf(message.msg_text,"%d",&pidHijos[j]); ///Cada hijo envía su pid y queda registrado en Server
               ///printf(", recibido y registrado :%d\n",pidHijos[j]);
            }
            ///En este punto todos los hijos CALCuladores están identificados
            ///printf("Server says:Todos los hijos CALCuladores están registrados\n");
            ///Partición del rango de búsqueda entre los hijos calculadores
            intervalo_hijo = (int) (RANGO_BUSQUEDA/numHijos);

            ///T_MSG_BUFFER messageLimits;
            message.msg_type = MSG_COD_LIMITES;
            for(j=0;j<numHijos;j++){
                base_hijo = BASE + (intervalo_hijo*j);
                limite_hijo = base_hijo + intervalo_hijo;
                if (j==(numHijos-1)){
                    limite_hijo = LIMITE+1;
                }
                sprintf(message.msg_text,"%ld %ld",base_hijo, limite_hijo);
                msgsnd(messageQueueId,&message, sizeof(message), IPC_NOWAIT);
                printf("Server says: send limits to my child%d, [%ld,%ld) \n",j,base_hijo,limite_hijo);

            }

            ///Presenta por pantalla la jerarquía de procesos mientras que los hijos están calculando
            imprimirJerarquiaPorcesos(rootPid,serverPid,pidHijos,numHijos);
            //sleep(30);
            ///Crea el fichero de resultados primos.txt
            if((ficheroSalida = fopen(NOMBRE_FICHERO_SALIDA,"w")) == NULL){
                perror("Error al crear el fichero de salida");
                exit(ERR_FSAL);
            }
            printf("Se ha creado el fichero de salida primos.txt\n");
            time(&startTime); //instante del inicio del registro de cómputo
            //Mientras haya procesos CALCuladores pendientes de finalizar
            ///T_MSG_BUFFER messageReceived;
            while(numeroCalculadoresFinalizados< numHijos){
                ///printf("Server says: A la espera de recibir los mesultados de computación\n");

                if(msgrcv(messageQueueId,&message, sizeof(message),0,0)==-1){
                    perror("Server: msgrcv failed\n");
                    exit(ERR_RECV);
                }
                ///printf("Mensjae de tipo %d %s\n",messageReceived.msg_type, messageReceived.msg_text);
                if (message.msg_type == MSG_COD_RESULTADOS){
                    int numeroPrimo;
                    //Dentro de la cadena diferenciamos entre el pidCalculador y el número primo encontrado
                    sscanf(message.msg_text,"%d %d",&pidCalculador,&numeroPrimo);
                    //Preparamos la línea informativa por la consola
                    sprintf(info,"MSG %ld : %s\n" ,++numerosPrimosEncontrados,message.msg_text);
                    informar(info,verbosity);
                    fprintf(ficheroSalida,"%d\n",numeroPrimo);
                    ///Si se ha recibido un número de primos dvisible por 5 se escribe en cuentaprimos.txt
                    if (numerosPrimosEncontrados % FRECUENCIA_ESCRITURA_CUENTA_PRIMOS ==0){
                        ficheroCuentaPrimos = fopen(NOMBRE_FICHERO_CUENTA_PRIMOS,"w");
                        fprintf(ficheroCuentaPrimos,"%ld\n",numerosPrimosEncontrados);
                        fclose(ficheroCuentaPrimos);
                    }
                } else if (message.msg_type == MSG_COD_FIN){
                    //Preparamos la linea informativa por la consola
                    numeroCalculadoresFinalizados++;
                    sprintf(info,"FIN %d %s\n",numeroCalculadoresFinalizados,message.msg_text);
                    informar(info,verbosity);
                } else {
                    perror("Server se ha encontrado con un mensaje no esperado\n");
                    exit(ERR_MSG_NOT_EXPECTED);
                }
            }
            time(&endTime); ///Instante final de registro de procesamiento
            double dif = difftime(endTime,startTime);
            printf("Server: Tiempo total de computación: %.2lf seconds,\n",dif);
            msgctl(messageQueueId, IPC_RMID, NULL);
            fflush(ficheroSalida);
            fclose(ficheroSalida);
            exit(0);
        }
    } else { //Rama raíz, soy el proceso primigenio
        alarm(INTERVALO_TIMER);
        signal(SIGALRM,alarmHandler);
        wait(NULL); //Espera a la finalización del Server
        printf("Resultado: %ld primos detectados\n",contarLineas());
        exit(0);
    }
}

int comprobarSiEsPrimo(long int numero) {
    if (numero < 2) return 0; // Por convenio 0 y 1 no son primos ni compuestos
    else
        for (int x = 2; x <= (numero / 2) ; x++)
            if (numero % x == 0) return 0;
    return 1;
}

void imprimirJerarquiaPorcesos(int pidRaiz, int pidServidor, int *pidHijos, int numHijos){
    printf("\n");
    printf("RAIZ\tSERV\tCALC\n"); ///Línea de cabecera separado por tabulación
    printf("%d\t%d\t%d\n",pidRaiz,pidServidor,pidHijos[0]);//Línea con proceso raíz, servidor y primer hijo
    for (int k=1; k < numHijos; k++){
        printf("\t\t%d\n",pidHijos[k]); ///Resto de hijos
    }
    printf("\n");
}
///Muestra por la consola el texto si verboso==1
void informar(char *texto, int verboso){
    if(verboso)
        printf("%s",texto);
}

///Contar las líneas del fichero primos.txt para saber cuántos se han encontrado
long int contarLineas(){
    long int count = 0;
    long int numeroPrimo;
    FILE *ficheroPrimos;
    ficheroPrimos = fopen(NOMBRE_FICHERO_SALIDA,"r");
    while(fscanf(ficheroPrimos,"%ld",&numeroPrimo)!=EOF)
        count++;
    fclose(ficheroPrimos);
    return count;
}



///Manejador de la alarma en el proceso RAÍZ
void alarmHandler(int signo){
    FILE *ficheroCuentaPrimos;
    int numeroPrimosEncontrados=0;
    computoTotalSegundos += INTERVALO_TIMER;
    if ((ficheroCuentaPrimos = fopen(NOMBRE_FICHERO_CUENTA_PRIMOS,"r"))!=NULL){
        fscanf(ficheroCuentaPrimos,"%d",&numeroPrimosEncontrados);
        fclose(ficheroCuentaPrimos);
    }
    printf("%02d (segs): %d primos encontrados\n",computoTotalSegundos,numeroPrimosEncontrados);
    alarm(INTERVALO_TIMER);
}
