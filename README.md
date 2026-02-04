# E-commerce-data-Engineering

Ce dépôt contient un projet de démonstration pour une architecture de traitement de données e‑commerce basée sur des fonctions AWS Lambda et un bucket S3. Il fournit un générateur de commandes (lambda) qui produit des fichiers JSON Lines dans un préfixe raw/ et un processeur (lambda) qui consomme ces fichiers, les enrichit et écrit des fichiers traités dans processed/.


## Composants principaux

- ecommerce-order-generator-function.py
  - Lambda qui génère des commandes réalistes et écrit un fichier JSONL dans s3://shopfast-ecommerce-data/raw/year=YYYY/month=MM/day=DD/
  - Paramètres configurables : BUCKET_NAME, PREFIX_RAW, NUM_ORDERS

- ecommerce-order-processor.py
  - Lambda déclenchée par un événement S3 (nouvel objet dans raw/) — lit le fichier JSONL, valide et enrichit chaque commande, puis écrit un fichier JSONL dans s3://shopfast-ecommerce-data/processed/year=YYYY/month=MM/day=DD/
  - Contient des règles de classification, validation et enrichissement (segments clients, priorité de livraison, analyse produit, etc.)

- lambda policy generator-foramtion intro-data.txt
  - Notes et exemples de politiques IAM et de configuration EventBridge

## Flux de données (résumé)

1. Le générateur crée N commandes et dépose un fichier JSONL dans raw/ (s3).
2. Un événement S3 (ObjectCreated) déclenche la Lambda de traitement (ou EventBridge selon la configuration).
3. Le processeur lit le fichier ligne par ligne, valide et enrichit les commandes.
4. Les commandes valides sont écrites en JSONL dans processed/ pour analyse ultérieure (Athena, Glue, Redshift, etc.).

## S3 - Structure recommandée

- shopfast-ecommerce-data/
  - raw/ (ingestion) — fichiers JSONL partitionnés par year/month/day
  - processed/ (données enrichies) — fichiers JSONL partitionnés par year/month/day

## Configuration et déploiement (rapide)

### Prérequis

- Compte AWS avec permissions pour créer lambdas, rôles IAM, S3, EventBridge
- AWS CLI configurée ou AWS Console

### IAM

- Créer un rôle Lambda pour ecommerce-order-generator avec permission s3:PutObject sur arn:aws:s3:::shopfast-ecommerce-data/*
- Créer un rôle Lambda pour ecommerce-order-processor avec permission s3:GetObject sur arn:aws:s3:::shopfast-ecommerce-data/raw/* et s3:PutObject sur arn:aws:s3:::shopfast-ecommerce-data/processed/*

### EventBridge / Trigger S3

- Exemple d'event pattern (EventBridge) pour déclencher la Lambda de traitement :

{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": { "name": ["shopfast-ecommerce-data"] },
    "object": { "key": [{ "prefix": "raw/" }] }
  }
}

- Activer l'input transformer pour mappler le payload EventBridge en format d'événement S3 (si utilisé).

### Exécution locale / Tests

- Pour tester le générateur localement, vous pouvez exécuter ecommerce-order-generator-function.py après avoir configuré des identifiants AWS valides et en modifiant NUM_ORDERS si nécessaire.
- Pour tester le processeur localement, préparez un fichier JSONL dans le même format et simulez l'événement S3 ou appelez directement la fonction process_order(order) avec des objets de test.

### Bonnes pratiques

- Utiliser des variables d'environnement pour BUCKET_NAME et préfixes (ne pas hardcoder en production)
- Implémenter des métriques CloudWatch (compteurs d'erreurs, temps de traitement, nombre de commandes traitées)
- Prévoir une stratégie de retry / DLQ (SQS ou SNS) pour les fichiers échoués
- Documenter les schémas de données pour Athena/Glue (colnames, types, partitions)

## Fichiers importants

- ecommerce-order-generator-function.py — génération et écriture JSONL
- ecommerce-order-processor.py — lecture JSONL, validation, enrichissement, écriture
- lambda policy generator-foramtion intro-data.txt — notes et templates IAM/EventBridge

## Contribution

Les contributions sont bienvenues. Pour proposer des améliorations :

1. Forkez le dépôt
2. Créez une branche feature/bugfix
3. Ouvrez une Pull Request expliquant les changements

## Licence

Ce projet est fourni sans licence explicite. Ajoutez un fichier LICENSE si vous souhaitez en fixer une (MIT, Apache-2.0, etc.).

## Contact

Pour toute question, ouvrez une issue ou contactez l'auteur du dépôt.
