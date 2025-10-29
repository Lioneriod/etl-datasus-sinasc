import fs from "fs";
import path from "path";
import csv from "csv-parser";
import { createObjectCsvWriter } from "csv-writer";
import { ensureDirs } from "./utils.js";

function normalizeRow(row) {
  return {
    id: row._id,
    notification_date: row.dataNotificacao?.split("T")[0] || "",
    state: row.estado?.trim().toUpperCase() || "",
    city: row.municipio?.trim() || "",
    covid_uti: parseInt(row.ocupacaoCovidUti || "0"),
    covid_cli: parseInt(row.ocupacaoCovidCli || "0"),
    confirmed_deaths: parseInt(row.saidaConfirmadaObitos || "0"),
    confirmed_discharges: parseInt(row.saidaConfirmadaAltas || "0"),
    excluded: row.excluido === "1" || row.excluido === "true",
    validated: row.validado === "1" || row.validado === "true",
    created_at: row._created_at || "",
    updated_at: row._updated_at || "",
  };
}

async function main() {
  ensureDirs();
  const infile = path.join(process.cwd(), "data", "bronze", "covid_raw.csv");
  const outfile = path.join(process.cwd(), "data", "silver", "covid_clean.csv");

  const writer = createObjectCsvWriter({
    path: outfile,
    header: [
      { id: "id", title: "id" },
      { id: "notification_date", title: "notification_date" },
      { id: "state", title: "state" },
      { id: "city", title: "city" },
      { id: "covid_uti", title: "covid_uti" },
      { id: "covid_cli", title: "covid_cli" },
      { id: "confirmed_deaths", title: "confirmed_deaths" },
      { id: "confirmed_discharges", title: "confirmed_discharges" },
      { id: "excluded", title: "excluded" },
      { id: "validated", title: "validated" },
      { id: "created_at", title: "created_at" },
      { id: "updated_at", title: "updated_at" },
    ],
  });

  const rows = [];
  fs.createReadStream(infile)
    .pipe(csv({ separator: "," }))
    .on("data", (data) => {
      const row = normalizeRow(data);
      if (row.state && row.notification_date) rows.push(row);
    })
    .on("end", async () => {
      await writer.writeRecords(rows);
      console.log("Silver layer written:", rows.length, "records");
    });
}

main().catch((e) => console.error(e));
