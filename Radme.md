Normalmente las visitas son a una location.
Las posiciones iniciales de los choferes (fábrica u hogar) cuentan como una visita dentro de la ruta.

Se revisó que normalmente el id de una visita viene dado por el id_location.

El chofer guarda dos objetos de location, uno para start y end de su jornada. Estas se consideran visitas dentro de una ruta (el viaje se optimiza de acuerdo a las condiciones iniciales, intermedias y finales). COmo un chofer puede participar en dos proyectos distintos, entre ambas rutas se repite la visita inicial y la final (misma location_id, que corresponde al mismo chofer mismo chofer)

Además, la ruta considera el tiempo de break como una visita con id='nowhere' y sin objeto location. Entonces se repite este id 'nowhere' se repite entre las rutas de y entre los choferes.

Rutas interesantes de la api:

