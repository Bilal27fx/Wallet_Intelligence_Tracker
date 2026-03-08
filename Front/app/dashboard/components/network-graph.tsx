"use client";

import { useRef, useEffect, useCallback, useState } from "react";
import type { NetworkNode, NetworkLink } from "@/lib/mock-data";

interface NetworkGraphProps {
  centerWallet: string;
  data: {
    nodes: NetworkNode[];
    links: NetworkLink[];
  };
  onNodeClick: (nodeId: string) => void;
}

const NODE_COLORS: Record<string, string> = {
  smart: "#00ff88",
  buy: "#2196f3",
  dump: "#f44336",
  cex: "#ffc107",
  unknown: "#666666",
};

const NODE_LABELS: Record<string, string> = {
  smart: "This Wallet",
  buy: "Buy Wallet",
  dump: "Dump Wallet",
  cex: "CEX",
  unknown: "Unknown",
};

interface SimNode extends NetworkNode {
  x: number;
  y: number;
  vx: number;
  vy: number;
  radius: number;
}

export function NetworkGraph({
  centerWallet,
  data,
  onNodeClick,
}: NetworkGraphProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const animRef = useRef<number>(0);
  const nodesRef = useRef<SimNode[]>([]);
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);
  const [tooltip, setTooltip] = useState<{
    x: number;
    y: number;
    node: SimNode;
  } | null>(null);
  const dragRef = useRef<{ node: SimNode | null; offsetX: number; offsetY: number }>({
    node: null,
    offsetX: 0,
    offsetY: 0,
  });
  const pulseRef = useRef(0);

  const getNodeRadius = useCallback(
    (node: NetworkNode) => {
      const maxVol = Math.max(...data.nodes.map((n) => n.volume), 1);
      const base = node.id === centerWallet ? 28 : 14;
      const scale = node.id === centerWallet ? 1 : 0.4 + (node.volume / maxVol) * 0.6;
      return base * scale;
    },
    [data.nodes, centerWallet]
  );

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * window.devicePixelRatio;
    canvas.height = rect.height * window.devicePixelRatio;

    const cx = rect.width / 2;
    const cy = rect.height / 2;

    // Initialize nodes
    const simNodes: SimNode[] = data.nodes.map((n, i) => {
      const angle = (2 * Math.PI * i) / data.nodes.length;
      const dist = n.id === centerWallet ? 0 : 120 + Math.random() * 60;
      return {
        ...n,
        x: cx + Math.cos(angle) * dist,
        y: cy + Math.sin(angle) * dist,
        vx: 0,
        vy: 0,
        radius: getNodeRadius(n),
      };
    });

    nodesRef.current = simNodes;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    function simulate() {
      const nodes = nodesRef.current;
      const dpr = window.devicePixelRatio;

      // Simple force simulation
      for (let iter = 0; iter < 3; iter++) {
        // Repulsion between all nodes
        for (let i = 0; i < nodes.length; i++) {
          for (let j = i + 1; j < nodes.length; j++) {
            const dx = nodes[j].x - nodes[i].x;
            const dy = nodes[j].y - nodes[i].y;
            const dist = Math.sqrt(dx * dx + dy * dy) || 1;
            const minDist = nodes[i].radius + nodes[j].radius + 40;
            if (dist < minDist) {
              const force = (minDist - dist) * 0.05;
              const fx = (dx / dist) * force;
              const fy = (dy / dist) * force;
              nodes[i].vx -= fx;
              nodes[i].vy -= fy;
              nodes[j].vx += fx;
              nodes[j].vy += fy;
            }
          }
        }

        // Attraction along links
        for (const link of data.links) {
          const source = nodes.find((n) => n.id === link.source);
          const target = nodes.find(
            (n) => n.id === (typeof link.target === "string" ? link.target : (link.target as unknown as SimNode).id)
          );
          if (!source || !target) continue;
          const dx = target.x - source.x;
          const dy = target.y - source.y;
          const dist = Math.sqrt(dx * dx + dy * dy) || 1;
          const idealDist = 160;
          const force = (dist - idealDist) * 0.008;
          const fx = (dx / dist) * force;
          const fy = (dy / dist) * force;
          source.vx += fx;
          source.vy += fy;
          target.vx -= fx;
          target.vy -= fy;
        }

        // Center gravity
        for (const node of nodes) {
          if (node.id === centerWallet && !dragRef.current.node) {
            node.vx += (cx - node.x) * 0.05;
            node.vy += (cy - node.y) * 0.05;
          } else if (!dragRef.current.node || dragRef.current.node.id !== node.id) {
            node.vx += (cx - node.x) * 0.002;
            node.vy += (cy - node.y) * 0.002;
          }
        }

        // Apply velocities
        for (const node of nodes) {
          if (dragRef.current.node && dragRef.current.node.id === node.id) continue;
          node.vx *= 0.85;
          node.vy *= 0.85;
          node.x += node.vx;
          node.y += node.vy;

          // Bounds
          node.x = Math.max(node.radius, Math.min(rect.width - node.radius, node.x));
          node.y = Math.max(node.radius, Math.min(rect.height - node.radius, node.y));
        }
      }

      // Draw
      ctx!.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx!.clearRect(0, 0, rect.width, rect.height);

      pulseRef.current += 0.03;
      const pulseScale = 1 + Math.sin(pulseRef.current) * 0.15;

      // Draw links
      for (const link of data.links) {
        const source = nodes.find((n) => n.id === link.source);
        const target = nodes.find(
          (n) => n.id === (typeof link.target === "string" ? link.target : (link.target as unknown as SimNode).id)
        );
        if (!source || !target) continue;

        const thickness = Math.max(1, Math.min(link.txCount * 0.8, 5));
        ctx!.beginPath();
        ctx!.moveTo(source.x, source.y);
        ctx!.lineTo(target.x, target.y);
        ctx!.strokeStyle = "rgba(255,255,255,0.12)";
        ctx!.lineWidth = thickness;
        ctx!.stroke();
      }

      // Draw nodes
      for (const node of nodes) {
        const color = NODE_COLORS[node.type] || "#666";
        const isCenter = node.id === centerWallet;
        const isHovered = node.id === hoveredNode;
        const r = isCenter ? node.radius * pulseScale : node.radius;

        // Glow
        if (isCenter || isHovered) {
          const gradient = ctx!.createRadialGradient(
            node.x,
            node.y,
            r * 0.5,
            node.x,
            node.y,
            r * 2.5
          );
          gradient.addColorStop(0, `${color}40`);
          gradient.addColorStop(1, `${color}00`);
          ctx!.beginPath();
          ctx!.arc(node.x, node.y, r * 2.5, 0, Math.PI * 2);
          ctx!.fillStyle = gradient;
          ctx!.fill();
        }

        // Node circle
        ctx!.beginPath();
        ctx!.arc(node.x, node.y, r, 0, Math.PI * 2);
        ctx!.fillStyle = isCenter ? color : `${color}cc`;
        ctx!.fill();

        if (isCenter) {
          ctx!.strokeStyle = color;
          ctx!.lineWidth = 2;
          ctx!.stroke();
        }

        // Label
        ctx!.fillStyle = "#fff";
        ctx!.font = `${isCenter ? "bold 10px" : "9px"} Inter, system-ui, sans-serif`;
        ctx!.textAlign = "center";
        ctx!.fillText(
          node.address.slice(0, 8) + "...",
          node.x,
          node.y + r + 14
        );
      }

      animRef.current = requestAnimationFrame(simulate);
    }

    simulate();

    return () => {
      cancelAnimationFrame(animRef.current);
    };
  }, [data, centerWallet, hoveredNode, getNodeRadius]);

  const getNodeAt = useCallback(
    (x: number, y: number): SimNode | null => {
      const nodes = nodesRef.current;
      for (let i = nodes.length - 1; i >= 0; i--) {
        const dx = nodes[i].x - x;
        const dy = nodes[i].y - y;
        if (dx * dx + dy * dy < (nodes[i].radius + 4) * (nodes[i].radius + 4)) {
          return nodes[i];
        }
      }
      return null;
    },
    []
  );

  const handleMouseMove = useCallback(
    (e: React.MouseEvent<HTMLCanvasElement>) => {
      const canvas = canvasRef.current;
      if (!canvas) return;
      const rect = canvas.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;

      if (dragRef.current.node) {
        dragRef.current.node.x = x;
        dragRef.current.node.y = y;
        dragRef.current.node.vx = 0;
        dragRef.current.node.vy = 0;
        return;
      }

      const node = getNodeAt(x, y);
      setHoveredNode(node?.id || null);
      if (node) {
        setTooltip({ x: e.clientX - rect.left, y: e.clientY - rect.top, node });
        canvas.style.cursor = "pointer";
      } else {
        setTooltip(null);
        canvas.style.cursor = "default";
      }
    },
    [getNodeAt]
  );

  const handleMouseDown = useCallback(
    (e: React.MouseEvent<HTMLCanvasElement>) => {
      const canvas = canvasRef.current;
      if (!canvas) return;
      const rect = canvas.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;
      const node = getNodeAt(x, y);
      if (node) {
        dragRef.current = { node, offsetX: 0, offsetY: 0 };
      }
    },
    [getNodeAt]
  );

  const handleMouseUp = useCallback(
    (e: React.MouseEvent<HTMLCanvasElement>) => {
      if (dragRef.current.node) {
        const canvas = canvasRef.current;
        if (!canvas) return;
        const rect = canvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const node = getNodeAt(x, y);
        if (node && node.id === dragRef.current.node.id) {
          onNodeClick(node.id);
        }
        dragRef.current = { node: null, offsetX: 0, offsetY: 0 };
      }
    },
    [getNodeAt, onNodeClick]
  );

  return (
    <div className="relative h-full w-full">
      <canvas
        ref={canvasRef}
        className="h-full w-full"
        onMouseMove={handleMouseMove}
        onMouseDown={handleMouseDown}
        onMouseUp={handleMouseUp}
      />

      {/* Tooltip */}
      {tooltip && (
        <div
          className="pointer-events-none absolute z-10 rounded-lg border border-[rgba(255,255,255,0.1)] bg-[#1a1a1a] px-3 py-2 shadow-lg"
          style={{
            left: tooltip.x + 12,
            top: tooltip.y - 10,
          }}
        >
          <p className="font-mono text-xs text-foreground">
            {tooltip.node.address}
          </p>
          <p className="text-[10px] text-muted-foreground">
            {NODE_LABELS[tooltip.node.type]} | Vol: $
            {(tooltip.node.volume / 1000).toFixed(0)}K
          </p>
        </div>
      )}

      {/* Legend */}
      <div className="absolute right-3 bottom-3 rounded-lg border border-[rgba(255,255,255,0.1)] bg-[rgba(10,10,10,0.9)] px-3 py-2">
        <p className="mb-1.5 text-[10px] font-semibold text-muted-foreground">
          LEGEND
        </p>
        {Object.entries(NODE_COLORS).map(([type, color]) => (
          <div key={type} className="flex items-center gap-2 py-0.5">
            <span
              className="h-2.5 w-2.5 rounded-full"
              style={{ background: color }}
            />
            <span className="text-[10px] text-muted-foreground">
              {NODE_LABELS[type]}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
