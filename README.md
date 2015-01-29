# es_docs_indexer
Programme scala qui indexe dans un cluster elasticsearch (entré en paramètre - $0) les fichiers présents dans une arborescence (elle aussi passée en paramètre du programme - $1).
Les options d'indexation sont les suivantes :
- le contenu des fichiers vont être indexés (nécessite l'installation préalable du plugin attachment sur le cluster elasticsearch)
- le contenu des fichiers sera stocké, intégralement.
Cela permet de rechercher des informations dans le contenu des fichiers avec elasticsearch, et d'afficher à les "highlights" (matches des mots-clés, placés dans leur contexte).