flowchart TB
    subgraph Cloud
      PG[(Timescale Cloud)]:::db
      click PG href "https://www.timescale.com/cloud"
      ECR[容器登錄<br>(ECR)]:::infra
      click ECR href "https://docs.aws.amazon.com/AmazonECR/latest/userguide/what-is-ecr.html"
      subgraph EKS Cluster
        ETL[ETL CronJob]:::svc
        API[Backend Deployment]:::svc
        MON[kube-prometheus]:::mon
      end
      FE[React Front-end<br>S3 + CloudFront]:::fe
      CI[GitHub Actions]:::infra
      CD[Argo CD]:::infra
    end
    classDef db fill:#b3d5ff,stroke:#0d47a1,color:#0d47a1;
    classDef svc fill:#dcedc8,stroke:#33691e,color:#33691e;
    classDef infra fill:#e0e0e0,stroke:#424242;
    classDef fe fill:#ffe0b2,stroke:#e64a19,color:#e64a19;
    classDef mon fill:#f8bbd0,stroke:#ad1457,color:#ad1457;
    CI--push image-->ECR
    CI--k8s manifests PR-->CD
    CD--sync-->EKS Cluster
    ETL--DSN(secret)-->PG
    API--DSN(secret)-->PG
    FE--HTTPS-->API
