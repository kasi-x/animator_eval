# Atlas schema management configuration
# This file defines the database schema and how it's managed via Atlas

variable "db_url" {
  type    = string
  default = env.ATLAS_DB_URL != null ? env.ATLAS_DB_URL : "sqlite://./data/animetor.db"
}

env "dev" {
  src = "src/models_v2.py"  # SQLModel schema as source of truth
  dev = var.db_url           # Development database
  
  migration {
    dir = "migrations/atlas"
  }
  
  format {
    migrate {
      diff = <<-EOM
        {{ sql . "  " }}
      EOM
    }
  }
}

env "prod" {
  src = "src/models_v2.py"
  src = "sqlite://./data/animetor_prod.db"  # Production database (read-only inspection)
  
  migration {
    dir = "migrations/atlas"
  }
}
