export default function SignalsPage() {
  return (
    <div className="p-6 flex flex-col gap-4">
      <div className="flex flex-col gap-1">
        <h1 className="text-2xl font-semibold tracking-tight">
          Tüm Sinyaller
        </h1>
        <p className="text-sm text-muted-foreground">
          Strateji konsensus tablosu — yakında
        </p>
      </div>

      <div className="rounded-lg border border-dashed p-10 flex flex-col items-center gap-3 text-center text-muted-foreground">
        <p className="text-sm max-w-md leading-relaxed">
          Bu sayfa Minervini + Carr + diğer stratejilerin kesişimini
          gösterecek.
        </p>
        <p className="text-xs">ADIM 5+ dolacak.</p>
      </div>
    </div>
  );
}
