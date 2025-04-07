using Microsoft.Azure.Cosmos;
using MvcCoreAzureCosmos.Models;

namespace MvcCoreAzureCosmos.Service
{
    public class ServiceCosmosDb
    {
        //DENTRO DE COSMOS SE TRABAJA CON CLIENT Y CONTAINERS
        //DENTRO DE LOS CONTAINERS ESTAN LOS ITEMS
        private CosmosClient clientCosmos;
        private Container containerCosmos;

        public ServiceCosmosDb(CosmosClient clientCosmos, Container containerCosmos)
        {
            this.clientCosmos = clientCosmos;
            this.containerCosmos = containerCosmos;
        }

        //METODO PARA CREAR NUESTRA BBDD Y DENTRO NUESTRO CONTAINER PARA LOS ITEMS
        public async Task CreateDatabaseAsync()
        {
            //CREAMOS LA BBDD
            await this.clientCosmos.CreateDatabaseIfNotExistsAsync("vehiculoscosmos");
            //DENTRO DE LA BBDD, CREAREMOS NUESTROS CONTAINERS
            ContainerProperties properties = new ContainerProperties("containercoches", "/id");
            //CREAMOS EL CONTAINER DENTRO DE NUESTRA BBDD
            await this.clientCosmos.GetDatabase("vehiculoscosmos").CreateContainerIfNotExistsAsync(properties);
        }

        //METODO PARA INSERTAR ELEMENTOS DENTRO DE COSMOS
        public async Task InsertCocheAsync(Coche car)
        {
            //EN EL MOMENTO DE INSERTAR, COSMOS NO SABE ASIGNAR AUTOMATICAMENTE 
            //SU PARTITION KEY, DEBEMOS DECIRSELO DE FORMA EXPLICITA
            await this.containerCosmos.CreateItemAsync<Coche>(car, new PartitionKey(car.Id));
        }

        public async Task<List<Coche>> GetCoches()
        {
            //UNA BBDD COSMOS NO SABE EL NUMERO DE REGISTROS REALES.
            //DEBEMOS LEER UTILIZANDO UN BUCLE while MIENTRAS QUE EXISTAN REGISTROS
            var query = this.containerCosmos.GetItemQueryIterator<Coche>();

            List<Coche> coches = new List<Coche>();
            while (query.HasMoreResults)
            {
                var results = await query.ReadNextAsync();
                //SON MULTIPLES COCHES LO QUE DEVUELVE, SE ALMACENA DENTRO DE NUESTRA COLECCION A LA VEZ
                coches.AddRange(results);
            }
            return coches;
        }

        public async Task UpdateCocheAsync(Coche car)
        {
            await this.containerCosmos.UpsertItemAsync<Coche>(car, new PartitionKey(car.Id));
        }

        public async Task DeleteCocheAsync(string id)
        {
            await this.containerCosmos.DeleteItemAsync<Coche>(id, new PartitionKey(id));
        }

        public async Task<Coche> FindCocheAsync(string id)
        {
            ItemResponse<Coche> response = 
                await this.containerCosmos.ReadItemAsync<Coche>(id, new PartitionKey(id));
            return response.Resource;
        }

        public async Task<List<Coche>> GetCochesMarcaAsync(string marca)
        {
            string sql = "select * from c where c.Marca='" + marca + "'";
            //PARA APLICAR LOS FILTROS SE UTILIZA UNA CLASE LLAMADA QueryDefinition
            QueryDefinition definition = new QueryDefinition(sql);
            var query = this.containerCosmos.GetItemQueryIterator<Coche>(definition);
            List<Coche> coches = new List<Coche>();
            while (query.HasMoreResults)
            {
                var results = await query.ReadNextAsync();
                coches.AddRange(results);
            }
            return coches;
        }
    }
}
