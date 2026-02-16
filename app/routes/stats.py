<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lotto AI | Quantum Pro Terminal</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700;800&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'JetBrains Mono', monospace; background-color: #020617; color: #e2e8f0; }
        .neon-border { box-shadow: 0 0 15px rgba(59, 130, 246, 0.2); border: 1px solid rgba(59, 130, 246, 0.3); }
        .consola-scrollbar::-webkit-scrollbar { width: 4px; }
        .consola-scrollbar::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 10px; }
        .pixel-grid { background-image: radial-gradient(#1e293b 1px, transparent 1px); background-size: 30px 30px; }
        @keyframes pulse-slow { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
        .scanning { animation: pulse-slow 2s infinite; }
    </style>
</head>
<body class="min-h-screen pixel-grid p-4 lg:p-8">

    <header class="max-w-7xl mx-auto flex flex-col md:flex-row justify-between items-start md:items-center mb-10 gap-6">
        <div>
            <h1 class="text-3xl font-black tracking-tighter text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-indigo-300">
                NEURAL ENGINE <span class="text-xs border border-blue-500 px-2 py-0.5 ml-2 rounded text-blue-400">V4.5 PRO</span>
            </h1>
            <p class="text-[10px] text-slate-500 uppercase tracking-[0.3em] mt-1">Sincronización Cuántica: 2018-2026</p>
        </div>
        
        <div class="flex gap-4">
            <div class="bg-slate-900/60 p-4 rounded-xl border border-slate-800/50 flex flex-col items-end min-w-[140px]">
                <span class="text-[10px] text-blue-400 font-bold uppercase tracking-wider">Efectividad Global</span>
                <span id="global-efectividad" class="text-2xl font-black text-white">0%</span>
            </div>
            <div class="bg-slate-900/60 p-4 rounded-xl border border-slate-800/50 flex flex-col items-end min-w-[140px]">
                <span class="text-[10px] text-slate-500 font-bold uppercase tracking-wider">Estado Sistema</span>
                <span class="text-sm text-green-400 font-bold flex items-center gap-2">
                    <span class="w-2 h-2 bg-green-500 rounded-full scanning shadow-[0_0_8px_#22c55e]"></span> OPERATIVO
                </span>
            </div>
        </div>
    </header>

    <main class="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-12 gap-8">                                         
