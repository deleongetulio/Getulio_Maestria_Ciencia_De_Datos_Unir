// =====================================================
//  Título de la asignación: ACTIVIDAD 1 – Uso de MongoDB
//  Nombre y Apellidos: Getulio Cesar De Leon Fernandez
//  Matrícula: 5882984 - 964830
//  Profesor: Barbaro Jorge Ferro Castro
//  Materia: MODAM Bases de Datos para Datos Masivos
//  Maestría: Maestría en Análisis y Visualización de Datos Masivos
//  Universidad: Universidad Internacional de La Rioja (UNIR)
//  Fecha de entrega: 8/12/25
// =====================================================

// -----------------------------------------------------
// PASO 1: CARGA DE DATOS (se ejecutó en consola, aquí solo documentado)
// -----------------------------------------------------
//1.1 Crear Base de datos llamada "miscelinea"
// mongoimport --verbose --db miscelanea --collection books --file "C:\...\act-2-books.json"
// mongoimport --verbose --db miscelanea --collection companies --file "C:\...\act-2-companies.json"

// -----------------------------------------------------
// PASO 2: EXPLORA LAS COLECCIONES
// -----------------------------------------------------

// Usamos la base de datos miscelanea
use miscelanea

// 2.1.1  Documento de ejemplo de la colección books (formato "bonito")
db.books.find().limit(1).pretty()

// 2.1.2  Documento de ejemplo de la colección companies (formato "bonito")
db.companies.find().limit(1).pretty()

// 2.2  Identificar todas las distintas categorías (categories) de books
db.books.distinct("categories")

// 2.3  Identificar los distintos estados (status) de books
db.books.distinct("status")

// Describe brevemente qué arroja la siguiente consulta: 
// 2.4  Consulta sobre longDescription entre "A" (incluida) y "B" (excluida)
//      Muestra solo title y longDescription

db.getCollection('books').find(
    {
    longDescription: {$gte: "A", $lt: "B"}},    // Todos los documentos cuya longDescription comienza con la letra A 
    {
    title: 1,                                   // Que muestre el título
    longDescription: 1                          // Que muestre el longDescription     
 }
)
/* 
La consulta busca dentro de la colección books todos los documentos cuya longDescription 
comienza con la letra A (ya que la condición $gte: "A” y $lt: "B” limita los resultados a ese rango alfabético).
Una vez filtrados, solo muestra los campos title y longDescription de cada libro.
*/

// Utiliza la condición de la consulta anterior para recuperar aquellos libros que posean exactamente 2 autores y que estén publicados. 
// Muestra solo los campos: title, longDescription, status y authors.

// 2.5  Misma condición, pero libros con exactamente 2 autores y publicados
//      (status = "PUBLISH"). Solo se muestran title, longDescription, status y authors
db.getCollection('books').find(
  {
    longDescription: { $gte: "A", $lt: "B" },  // La condición de la consulta anterior
    authors: { $size: 2 },                     // Que tengan exactamente 2 autores
    status: "PUBLISH"                          // Libros publicados
  },
  {
    title: 1,                                  // Que muestre el título
    longDescription: 1,                        // Que muestre el longDescription                     
    status: 1,                                 // Que muestre el estado (status)
    authors: 1,                                // Que muestre los autores
    _id: 0                                     // Ocultamos el id
  }
)

// 2.6  Versión anterior utilizando .toArray()

db.getCollection('books').find(
  {
    longDescription: { $gte: "A", $lt: "B" },   // La condición de la consulta anterior
    authors: { $size: 2 },                      // Que tengan exactamente 2 autores 
    status: "PUBLISH"                           // Libros publicados     
  },
  {
    title: 1,                                   // Que muestre el título
    longDescription: 1,                         // Que muestre el longDescription                
    status: 1,                                  // Que muestre el estado (status)
    authors: 1,                                 // Que muestre los autores
    _id: 0                                      // Ocultamos el id
  }
).toArray()                                    // Convierte el cursor en un arreglo de documentos en memoria
/*  
.toArray() ejecuta el cursor devuelto por find() y convierte todos los documentos resultantes en un arreglo de JavaScript en memoria. 
En lugar de mostrar los documentos uno a uno como un cursor, devuelve un solo arreglo que contiene todos los documentos de la consulta.
*/

// 2.7  Versión anterior utilizando .forEach() y print()
//      (Recorre cada documento y construye una cadena personalizada)
db.getCollection('books').find(
  {
    longDescription: { $gte: "A", $lt: "B" },   // La condición de la consulta anterior   
    authors: { $size: 2 },                      // Que tengan exactamente 2 autores  
    status: "PUBLISH"                           // Libros publicados     
  },
  {
    title: 1,                                   // Que muestre el título
    longDescription: 1,                         // Que muestre el longDescription  
    status: 1,                                  // Que muestre el estado 
    authors: 1,                                 // Que muestre los autores 
    _id: 0                                      // Ocultamos el id 
  }
)
.toArray()                                      // Convierte el cursor en un arreglo de documentos en memoria
.forEach(function (valor, indice, array) {
  print(
    "Título: " + valor.title +                  // Muestra el título 
    " Autor 1: " + valor.authors[0] +           // Muestra el primer autor (se corrigió error del enunciado)   
    " Autor 2: " + valor.authors[1] +           // Muestra el segundo autor
    " Registro No.: " + indice                  // Muestra el número de registro según su orden
  );
});

/*
Esto es útil para mostrar la información de una manera más simplificada y legible.
Permite generar reportes o listados personalizados, mostrando únicamente la información relevante.
También permite procesar los datos de forma secuencial y mostrar información resumida sin imprimir el documento completo.
*/

// -----------------------------------------------------
// PASO 3: CONSULTAS SOBRE LA COLECCIÓN books
// -----------------------------------------------------

// 3.1  Tamaño de la colección books (en bytes)
db.books.dataSize() // 517,474 bytes

// 3.2  Número total de libros en la colección
db.books.countDocuments() // 431 libros en total en la colección

// 3.3  Libros con 200 o más páginas
db.books.countDocuments({ pageCount: { $gte: 200 } }) // 264 libros con 200 o más páginas 

// 3.4  Libros con entre 300 y 600 páginas [300, 600]
db.books.countDocuments({ pageCount: { $gte: 300, $lte: 600 } }) // 215 libros con páginas entre 300 y 600 páginas.

// 3.5.1  Libros con 0 páginas
db.books.countDocuments({ pageCount: 0 }) // 166 libros con 0 páginas

// 3.5.2  Libros con número de páginas distinto de 0
db.books.countDocuments({ pageCount: { $ne: 0 } }) // 265 libros con páginas distintas de 0

// 3.6.1  Libros publicados (status = "PUBLISH")
db.books.countDocuments({ status: "PUBLISH" }) // 363 libros publicados 

// 3.6.2  Libros no publicados (status distinto de "PUBLISH")
db.books.countDocuments({ status: { $ne: "PUBLISH" } }) // 68 libros no publicados

// -----------------------------------------------------
// PASO 4: CONSULTAS SOBRE LA COLECCIÓN companies
// -----------------------------------------------------

// 4.1  Tamaño de la colección companies (en bytes)
db.companies.dataSize() // 72,236,994 bytes

// 4.2  Número total de compañías
db.companies.countDocuments() // 18,801 compañías

// 4.3  Compañías fundadas en 1996, 1997, 2001 y 2005
db.companies.countDocuments({ founded_year: 1996 }) // 216 compañías fundadas en 1996
db.companies.countDocuments({ founded_year: 1997 }) // 200 compañías fundadas en 1997
db.companies.countDocuments({ founded_year: 2001 }) // 464 compañías fundadas en 2001
db.companies.countDocuments({ founded_year: 2005 }) // 961 compañías fundadas en 2005

// 4.4  Compañías que se dedican a "web" o "mobile"
//      Recupera: nombre, descripción, número de empleados, e-mail,
//      año, mes y día de su fundación.
db.companies.find(
  { category_code: { $in: ["web", "mobile"] } },    // Encuentra las compañías que se dedican a "web" o "mobile" 
  {
    name: 1,                                        // Nombre
    description: 1,                                 // Descripción 
    number_of_employees: 1,                         // Número de empleados
    email_address: 1,                               // Correo electrónico
    founded_year: 1,                                // Año de fundación
    founded_month: 1,                               // Mes de fundación
    founded_day: 1,                                 // Día de fundación
    _id: 0                                          // Ocultar id
  }
)

// 4.5  Compañías de videojuegos ("games_video")
//      Ordenadas de forma descendente por año de fundación
db.companies.find(
  { category_code: "games_video" },             // Encuentra compañías de videojuegos
  { name: 1, founded_year: 1, _id: 0 }          // Muestra nombre y año de fundación
).sort({ founded_year: -1 })                    // Ordenadas de forma descendente

// 4.6  ¿Cuántas compañías tienen 600 o más empleados?
db.companies.countDocuments({ number_of_employees: { $gte: 600 } }) // 303 compañías con 600 o más empleados

// 4.7  Compañías fundadas entre 2001 y 2005 (incluidos),
//      con 500 o más empleados, dedicadas a videojuegos o música.
//      Recupera: nombre, URL, usuario de Twitter, número de empleados.
db.companies.find(
  {
    founded_year: { $gte: 2001, $lte: 2005 },         // Entre 2001 y 2005 (incluidos)
    number_of_employees: { $gte: 500 },               // 500 o más empleados
    category_code: { $in: ["games_video", "music"] }  // Videojuegos o música
  },
  {
    name: 1,                   // Nombre
    homepage_url: 1,           // URL
    twitter_username: 1,       // Usuario de Twitter
    number_of_employees: 1,    // Número de empleados
    _id: 0                      
  }
)
// Existen dos compañias: "GREE" y "Bigpoint"


// ¿Alguna empresa se dedica a videojuegos y a la música a la vez?
db.companies.find({
  category_code: { $all: ["games_video", "music"] } // No existe ninguna, Esto se debe a que en el dataset dicho campo no es un array, sino un valor string
})


// 4.8  Empresas con única y exclusivamente 2 oficinas en San Francisco.
//
//      Como esto implica contar las oficinas por cada documento,
//      se utiliza un cursor + forEach para filtrar manualmente.
db.companies.find(
  { "offices.city": "San Francisco" }    // Filtra empresas que tengan al menos una oficina en San Francisco
)
.forEach(function(company) {              // Recorre cada empresa del resultado, una por una

  // Cuenta cuántas de sus oficinas están realmente en San Francisco
  let count = company.offices             // Accede al arreglo "offices"
    .filter(o => o.city === "San Francisco")  // Filtra oficinas en San Francisco
    .length;                               // Cantidad de oficinas encontradas

  if (count === 2) {                         // Si la empresa tiene exactamente 2 oficinas en San Francisco

    print({                                  // Imprime las compañías con 2 oficinas en San Francisco
      name: company.name                    // Muestra el nombre de la empresa: "Constant Contact", "GoGrid", "Notifixious"            
    });
  }
});




// 4.9  Empresas de videojuegos que:
//      - hayan sido adquiridas en 2007,
//      - por un precio >= 10 millones,
//      - y tengan oficinas en la ciudad de Culver City.
//      Recupera nombre y fecha de adquisición.
db.companies.find(
  {
    category_code: "games_video",              // Compañías de videojuegos
    "offices.city": "Culver City",             // Oficinas en Culver City
    acquisitions: {
      $elemMatch: {                            // Busca coincidencias en el arreglo acquisitions
        acquired_year: 2007,                   // Año 2007
        price_amount: { $gte: 10000000 }       // Precio >= 10 millones
      }
    }
  },
  {
    name: 1,                                   // Nombre
    "acquisitions.acquired_month": 1,          // Mes de adquisición
    "acquisitions.acquired_day": 1,            // Día de adquisición
    _id: 0                                     // Ocultar id
  }                                            // Ninguna empresa cumple estos requisitos
)

// Compruebo que todos los filtros funcionan correctamente por separado:
db.companies.countDocuments({ category_code: "games_video" })       // 1083 empresas de videojuegos
db.companies.countDocuments({ "offices.city": "Culver City" })      // 18 empresas con oficinas en Culver City
db.companies.countDocuments({acquisitions: {$elemMatch: { acquired_year: 2007, price_amount: { $gte: 10000000 }}}})   // 62 empresas adquiridas en 2007 por un precio mayor o igual a 10,000,000

/*
Tras ejecutar la consulta indicada, el resultado fue 0 documentos.
Esto se debe a que, aunque existen 1083 empresas de videojuegos, 18 empresas con oficinas en Culver City y 62 empresas adquiridas en 2007 por un precio igual o superior a 10 millones de dólares, 
no existe ninguna empresa que pertenezca simultáneamente a los tres grupos.
Por tanto, la consulta correctamente devuelta es el conjunto vacío.
*/

/* Comprobando el codigo de la 4.9:*/
db.companies.find(
  {
    category_code: "games_video",              // Compañías de videojuegos
    "offices.city": "Burbank",             // Oficinas en Culver City
    acquisitions: {
      $elemMatch: {                            // Busca coincidencias en el arreglo acquisitions
        acquired_year: 2007,                   // Año 2007
        price_amount: { $gte: 10000000 }       // Precio >= 10 millones
      }
    }
  },
  {
    name: 1,                                   // Nombre
    "acquisitions.acquired_month": 1,          // Mes de adquisición
    "acquisitions.acquired_day": 1,            // Día de adquisición
    _id: 0                                     // Ocultar id
  }                                            // Resultado: The Walt Disney Company.
)
/*
Para verificar que el código esté correcto, cambiamos la ciudad de Culver City a Burbank, y ahora sí vemos que solo una compañía cumple con estas condiciones: The Walt Disney Company.
*/

