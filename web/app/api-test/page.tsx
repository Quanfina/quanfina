"use client";

import { useQuery } from "@tanstack/react-query";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

interface HealthResponse {
  status: string;
  service: string;
  timestamp: string;
  db_connected: boolean;
}

async function fetchHealth(): Promise<HealthResponse> {
  const res = await fetch("/api/health");
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export default function ApiTestPage() {
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ["health"],
    queryFn: fetchHealth,
    refetchInterval: 15_000,
  });

  return (
    <div className="min-h-screen p-8 flex flex-col gap-6 font-sans">
      <h1 className="text-2xl font-semibold tracking-tight">
        POC ADIM 2 — FastAPI Health Check
      </h1>

      {isLoading && (
        <p className="text-muted-foreground text-sm animate-pulse">
          Bağlanıyor...
        </p>
      )}

      {isError && (
        <Card className="border-destructive">
          <CardHeader>
            <CardTitle className="text-destructive text-base flex items-center gap-2">
              <span
                className="h-2 w-2 rounded-full"
                style={{ backgroundColor: "var(--mtp-danger)" }}
              />
              Bağlantı Hatası
            </CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-2">
            <p className="font-mono text-sm">{String(error)}</p>
            <p className="text-sm text-muted-foreground">
              FastAPI çalışıyor mu?{" "}
              <code className="font-mono bg-muted px-1 rounded">
                .venv\Scripts\python.exe -m uvicorn main:app --reload --port 8000
              </code>
            </p>
          </CardContent>
        </Card>
      )}

      {data && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <span
                className="h-2.5 w-2.5 rounded-full"
                style={{
                  backgroundColor: data.db_connected
                    ? "var(--mtp-excellent)"
                    : "var(--mtp-danger)",
                }}
              />
              {data.service}
              <Badge variant={data.status === "ok" ? "default" : "destructive"}>
                {data.status}
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-2 font-mono text-sm">
            <Row label="db_connected">
              <span
                style={{
                  color: data.db_connected
                    ? "var(--mtp-excellent)"
                    : "var(--mtp-danger)",
                }}
              >
                {String(data.db_connected)}
              </span>
            </Row>
            <Row label="timestamp">{data.timestamp}</Row>
            <Row label="endpoint">/api/health</Row>
          </CardContent>
        </Card>
      )}

      <button
        onClick={() => void refetch()}
        className="text-sm text-muted-foreground hover:text-foreground underline w-fit"
      >
        Yenile
      </button>
    </div>
  );
}

function Row({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex gap-4">
      <span className="text-muted-foreground w-32">{label}</span>
      <span>{children}</span>
    </div>
  );
}
