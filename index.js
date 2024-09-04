const { Client } = require("pg");
const sql = require("mssql");
const dotenv = require("dotenv");

dotenv.config();

const mssqlConfig = {
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASSWORD,
  server: process.env.MSSQL_SERVER,
  database: process.env.MSSQL_DB,
  options: {
    trustServerCertificate: true,
    port: 1433,
  },
};

const pgClient = new Client({
  user: process.env.POSTGRE_USER,
  host: process.env.POSTGRE_HOST,
  database: process.env.POSTGRE_DB,
  password: process.env.POSTGRE_PASSWORD,
  port: 5432,
});

async function extractSchema() {
  let schema = "";
  try {
    await sql.connect(mssqlConfig);

    const tables =
      await sql.query`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'`;
    for (let table of tables.recordset) {
      const tableName = table.TABLE_NAME;

      const columns = await sql.query`
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ${tableName}`;

      let createTableQuery = `CREATE TABLE ${tableName} (\n`;

      columns.recordset.forEach((col, index) => {
        let dataType = col.DATA_TYPE;
        let nullable = col.IS_NULLABLE === "YES" ? "NULL" : "NOT NULL";

        switch (dataType) {
          case "int":
            dataType = "INTEGER";
            break;
          case "nvarchar":
          case "varchar":
            dataType = `VARCHAR(${col.CHARACTER_MAXIMUM_LENGTH})`;
            break;
          case "datetime":
          case "smalldatetime":
            dataType = "TIMESTAMP";
            break;
          case "date":
            dataType = "DATE";
            break;
          case "bit":
            dataType = "BOOLEAN";
            break;
          case "uniqueidentifier":
            dataType = "UUID";
            break;
          case "float":
            dataType = "DOUBLE PRECISION";
            break;
          case "decimal":
          case "numeric":
            dataType = `NUMERIC(${col.NUMERIC_PRECISION}, ${col.NUMERIC_SCALE})`;
            break;
          default:
            console.log(
              `Data type ${dataType} not explicitly handled, keeping as ${dataType}`
            );
        }

        createTableQuery += `    ${col.COLUMN_NAME} ${dataType} ${nullable}`;
        if (col.COLUMN_DEFAULT) {
          createTableQuery += ` DEFAULT ${col.COLUMN_DEFAULT}`;
        }

        if (index < columns.recordset.length - 1) {
          createTableQuery += ",\n";
        }
      });

      const primaryKey = await sql.query`
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_NAME = ${tableName} AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1`;

      if (primaryKey.recordset.length > 0) {
        createTableQuery += `,\n    PRIMARY KEY (${primaryKey.recordset
          .map((pk) => pk.COLUMN_NAME)
          .join(", ")})`;
      }

      createTableQuery += "\n);\n";
      schema += createTableQuery;
    }
  } catch (err) {
    console.error("Error extracting schema:", err);
  } finally {
    await sql.close();
  }

  try {
    await pgClient.query(schema);
    console.log("Schema successfully applied in PostgreSQL");
  } catch (err) {
    console.error("Error applying schema:", err);
  }
}

async function migrateData(batchSize = 1000) {
  try {
    await sql.connect(mssqlConfig);

    const tables =
      await sql.query`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'`;

    for (let table of tables.recordset) {
      const tableName = table.TABLE_NAME;
      const data = await sql.query(`SELECT * FROM ${tableName}`);

      if (data.recordset.length > 0) {
        const columns = Object.keys(data.recordset[0]).join(", ");
        
        for (let i = 0; i < data.recordset.length; i += batchSize) {
          const batch = data.recordset.slice(i, i + batchSize);
          const valueQueries = [];
          const valuesArray = [];

          batch.forEach((row) => {
            const entries = Object.entries(row);
            const values = entries.map(([key, val], index) => `$${valuesArray.length + index + 1}`).join(", ");
            const valueArray = entries.map(([key, val]) => val);

            valueQueries.push(`(${values})`);
            valuesArray.push(...valueArray);
          });

          const insertQuery = `INSERT INTO ${tableName} (${columns}) VALUES ${valueQueries.join(", ")};`;

          console.log(`Executing bulk insert query: ${insertQuery}`);
          
          try {
            await pgClient.query(insertQuery, valuesArray);
            console.log(`Migrated batch of data for table ${tableName}`);
          } catch (err) {
            console.error(`Error executing bulk insert for table ${tableName}:`, err);
          }
        }
      } else {
        console.log(`No data found for table ${tableName}`);
      }
    }
  } catch (err) {
    console.error("Error migrating data:", err);
  } finally {
    await sql.close();
  }
}

async function migrateForeignKeys() {
  try {
    await sql.connect(mssqlConfig);

    const foreignKeys = await sql.query(`
      SELECT 
        fk.name AS FK_NAME,
        tp.name AS TABLE_NAME,
        cp.name AS COLUMN_NAME,
        tr.name AS REFERENCED_TABLE_NAME,
        cr.name AS REFERENCED_COLUMN_NAME
      FROM 
        sys.foreign_keys AS fk
        INNER JOIN sys.foreign_key_columns AS fkc 
            ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.tables AS tp 
            ON fkc.parent_object_id = tp.object_id
        INNER JOIN sys.columns AS cp 
            ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
        INNER JOIN sys.tables AS tr 
            ON fkc.referenced_object_id = tr.object_id
        INNER JOIN sys.columns AS cr 
            ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
    `);

    for (let fk of foreignKeys.recordset) {
      const { FK_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME } = fk;
      
      const addForeignKeyQuery = `
        ALTER TABLE ${TABLE_NAME}
        ADD CONSTRAINT ${FK_NAME}
        FOREIGN KEY (${COLUMN_NAME})
        REFERENCES ${REFERENCED_TABLE_NAME}(${REFERENCED_COLUMN_NAME})
      `;
            
      try {
        await pgClient.query(addForeignKeyQuery);
      } catch (err) {
        console.error(`Error adding foreign key ${FK_NAME}:`, err);
      }
    }
  } catch (err) {
    console.error("Error migrating foreign keys:", err);
  } finally {
    await sql.close();
  }
}

async function migrateSchemaAndData() {
  try {
    await pgClient.connect();
    await extractSchema();
    await migrateData();
    await migrateForeignKeys();
  } catch (err) {
    console.error("Migration failed:", err);
  } finally {
    await pgClient.end();
  }
}

migrateSchemaAndData();
