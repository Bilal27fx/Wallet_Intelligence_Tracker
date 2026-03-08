export interface WalletInteraction {
  address: string;
  txCount: number;
  volume: number;
  role: string;
}

export interface CexInteraction {
  address: string;
  name: string;
  txCount: number;
  volume: number;
}

export interface UnknownInteraction {
  address: string;
  txCount: number;
  volume: number;
}

export interface WalletData {
  address: string;
  qualityScore: number;
  winRate: number;
  roi: number;
  entryPrice: number;
  positionSize: number;
}

export interface WalletDetails {
  address: string;
  qualityScore: number;
  winRate: number;
  roi: number;
  metrics: {
    totalInvested: number;
    totalRealized: number;
    currentHoldings: number;
    totalTrades: number;
    winningTrades: number;
    losingTrades: number;
    avgWin: number;
    avgLoss: number;
    bestTrade: number;
    worstTrade: number;
  };
  currentPosition: {
    token: string;
    entryPrice: number;
    currentPrice: number;
    size: number;
    unrealizedPnl: number;
    entryDate: string;
    holdingTime: string;
  };
  interactions: {
    buyWallets: WalletInteraction[];
    dumpWallets: WalletInteraction[];
    cex: CexInteraction[];
    unknown: UnknownInteraction[];
  };
}

export interface NetworkNode {
  id: string;
  type: 'smart' | 'buy' | 'dump' | 'cex' | 'unknown';
  address: string;
  volume: number;
}

export interface NetworkLink {
  source: string;
  target: string;
  txCount: number;
  volume: number;
}

export interface ConsensusGroup {
  id: string;
  score: number;
  strength: 'STRONG' | 'MODERATE' | 'WEAK';
  token: {
    symbol: string;
    chain: string;
    address: string;
    contractAddress: string;
    age: string;
    createdAt: string;
  };
  metrics: {
    walletsCount: number;
    totalCapital: number;
    avgEntryPrice: number;
    bestROI: number;
    worstROI: number;
    firstDetection: string;
    consensusType: string;
    detectionDate: string;
    intervalMin: string;
    intervalMax: string;
  };
  wallets: WalletData[];
  performance7d: Array<{ date: string; value: number }>;
  detectedAt: string;
}

export const tickerData = [
  { symbol: 'TURBO', change: 43.1 },
  { symbol: 'FLOKI', change: 38.0 },
  { symbol: 'AERO', change: 28.4 },
  { symbol: 'PEPE', change: 22.7 },
  { symbol: 'DEGEN', change: 18.3 },
  { symbol: 'MOG', change: 15.9 },
  { symbol: 'WIF', change: -5.2 },
  { symbol: 'BONK', change: 12.4 },
  { symbol: 'BRETT', change: -8.7 },
  { symbol: 'NEIRO', change: 9.1 },
  { symbol: 'POPCAT', change: 31.2 },
  { symbol: 'MYRO', change: -3.4 },
];

export const mockConsensusData: ConsensusGroup[] = [
  {
    id: 'consensus-1',
    score: 94,
    strength: 'STRONG',
    token: { symbol: 'TURBO', chain: 'ETH', address: '0x1234...abcd', contractAddress: '0xa35923162c49cF95e6BF26623385eb431ad920D3', age: '14d 7h', createdAt: '2026-02-11' },
    metrics: {
      walletsCount: 7,
      totalCapital: 482000,
      avgEntryPrice: 0.0087,
      bestROI: 234.7,
      worstROI: 79,
      firstDetection: '2026-02-25T14:23:00Z',
      consensusType: 'ACCUMULATION',
      detectionDate: '2026-02-25 14:23 UTC',
      intervalMin: '2m 34s',
      intervalMax: '18m 12s',
    },
    wallets: [
      { address: '0x7a3b...f56', qualityScore: 92, winRate: 91, roi: 234.7, entryPrice: 0.0085, positionSize: 68000 },
      { address: '0x2e1c...a89', qualityScore: 88, winRate: 85, roi: 187.3, entryPrice: 0.0089, positionSize: 45000 },
      { address: '0x9f4d...c12', qualityScore: 85, winRate: 88, roi: 156.2, entryPrice: 0.0087, positionSize: 52000 },
      { address: '0x1a6f...d67', qualityScore: 81, winRate: 82, roi: 134.5, entryPrice: 0.0091, positionSize: 73000 },
      { address: '0x4c2d...b90', qualityScore: 79, winRate: 79, roi: 112.8, entryPrice: 0.0088, positionSize: 89000 },
      { address: '0x8d7e...a23', qualityScore: 76, winRate: 76, roi: 97.4, entryPrice: 0.0092, positionSize: 94000 },
      { address: '0x3f5a...e78', qualityScore: 73, winRate: 74, roi: 79.0, entryPrice: 0.0090, positionSize: 61000 },
    ],
    performance7d: [
      { date: '2026-02-18', value: 100 },
      { date: '2026-02-19', value: 108 },
      { date: '2026-02-20', value: 115 },
      { date: '2026-02-21', value: 135 },
      { date: '2026-02-22', value: 142 },
      { date: '2026-02-23', value: 158 },
      { date: '2026-02-24', value: 165 },
    ],
    detectedAt: '1h ago',
  },
  {
    id: 'consensus-2',
    score: 87,
    strength: 'STRONG',
    token: { symbol: 'AERO', chain: 'BASE', address: '0x9876...efgh', contractAddress: '0x940181a94A35A4569E4529A3CDfB74e38FD98631', age: '8d 3h', createdAt: '2026-02-17' },
    metrics: {
      walletsCount: 5,
      totalCapital: 312000,
      avgEntryPrice: 1.24,
      bestROI: 178.2,
      worstROI: 56,
      firstDetection: '2026-02-25T12:10:00Z',
      consensusType: 'ACCUMULATION',
      detectionDate: '2026-02-25 12:10 UTC',
      intervalMin: '4m 12s',
      intervalMax: '22m 45s',
    },
    wallets: [
      { address: '0xab12...cd3', qualityScore: 90, winRate: 89, roi: 178.2, entryPrice: 1.21, positionSize: 78000 },
      { address: '0xde45...fg6', qualityScore: 86, winRate: 83, roi: 145.6, entryPrice: 1.25, positionSize: 62000 },
      { address: '0xhi78...jk9', qualityScore: 82, winRate: 80, roi: 112.3, entryPrice: 1.23, positionSize: 54000 },
      { address: '0xlm01...no2', qualityScore: 78, winRate: 77, roi: 89.7, entryPrice: 1.26, positionSize: 65000 },
      { address: '0xpq34...rs5', qualityScore: 74, winRate: 73, roi: 56.0, entryPrice: 1.28, positionSize: 53000 },
    ],
    performance7d: [
      { date: '2026-02-18', value: 100 },
      { date: '2026-02-19', value: 112 },
      { date: '2026-02-20', value: 125 },
      { date: '2026-02-21', value: 118 },
      { date: '2026-02-22', value: 138 },
      { date: '2026-02-23', value: 152 },
      { date: '2026-02-24', value: 148 },
    ],
    detectedAt: '3h ago',
  },
  {
    id: 'consensus-3',
    score: 78,
    strength: 'MODERATE',
    token: { symbol: 'PEPE', chain: 'ETH', address: '0xabcd...1234', contractAddress: '0x6982508145454Ce325dDbE47a25d4ec3d2311933', age: '1y 9m', createdAt: '2024-05-14' },
    metrics: {
      walletsCount: 6,
      totalCapital: 256000,
      avgEntryPrice: 0.00001245,
      bestROI: 145.3,
      worstROI: 34,
      firstDetection: '2026-02-25T09:45:00Z',
      consensusType: 'ACCUMULATION',
      detectionDate: '2026-02-25 09:45 UTC',
      intervalMin: '6m 08s',
      intervalMax: '35m 20s',
    },
    wallets: [
      { address: '0xtu67...vw8', qualityScore: 87, winRate: 86, roi: 145.3, entryPrice: 0.00001220, positionSize: 56000 },
      { address: '0xxy90...za1', qualityScore: 83, winRate: 81, roi: 123.7, entryPrice: 0.00001240, positionSize: 48000 },
      { address: '0xbc23...de4', qualityScore: 79, winRate: 78, roi: 98.4, entryPrice: 0.00001250, positionSize: 42000 },
      { address: '0xfg56...hi7', qualityScore: 75, winRate: 74, roi: 76.1, entryPrice: 0.00001260, positionSize: 38000 },
      { address: '0xjk89...lm0', qualityScore: 71, winRate: 70, roi: 52.8, entryPrice: 0.00001270, positionSize: 40000 },
      { address: '0xno12...pq3', qualityScore: 68, winRate: 67, roi: 34.0, entryPrice: 0.00001280, positionSize: 32000 },
    ],
    performance7d: [
      { date: '2026-02-18', value: 100 },
      { date: '2026-02-19', value: 105 },
      { date: '2026-02-20', value: 118 },
      { date: '2026-02-21', value: 124 },
      { date: '2026-02-22', value: 130 },
      { date: '2026-02-23', value: 125 },
      { date: '2026-02-24', value: 138 },
    ],
    detectedAt: '6h ago',
  },
  {
    id: 'consensus-4',
    score: 72,
    strength: 'MODERATE',
    token: { symbol: 'DEGEN', chain: 'BASE', address: '0x5678...ijkl', contractAddress: '0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed', age: '3d 18h', createdAt: '2026-02-22' },
    metrics: {
      walletsCount: 4,
      totalCapital: 189000,
      avgEntryPrice: 0.0156,
      bestROI: 112.5,
      worstROI: 28,
      firstDetection: '2026-02-25T07:30:00Z',
      consensusType: 'MIXED',
      detectionDate: '2026-02-25 07:30 UTC',
      intervalMin: '1m 56s',
      intervalMax: '12m 03s',
    },
    wallets: [
      { address: '0xrs45...tu6', qualityScore: 84, winRate: 82, roi: 112.5, entryPrice: 0.0150, positionSize: 56000 },
      { address: '0xvw78...xy9', qualityScore: 78, winRate: 76, roi: 87.3, entryPrice: 0.0158, positionSize: 48000 },
      { address: '0xza01...bc2', qualityScore: 72, winRate: 71, roi: 56.8, entryPrice: 0.0160, positionSize: 45000 },
      { address: '0xde34...fg5', qualityScore: 66, winRate: 65, roi: 28.0, entryPrice: 0.0162, positionSize: 40000 },
    ],
    performance7d: [
      { date: '2026-02-18', value: 100 },
      { date: '2026-02-19', value: 103 },
      { date: '2026-02-20', value: 110 },
      { date: '2026-02-21', value: 115 },
      { date: '2026-02-22', value: 108 },
      { date: '2026-02-23', value: 118 },
      { date: '2026-02-24', value: 124 },
    ],
    detectedAt: '8h ago',
  },
  {
    id: 'consensus-5',
    score: 65,
    strength: 'WEAK',
    token: { symbol: 'MOG', chain: 'ETH', address: '0xmnop...5678', contractAddress: '0xaaeE1A9723aaDB7afA2810263653A34bA2C21C7a', age: '1d 2h', createdAt: '2026-02-24' },
    metrics: {
      walletsCount: 3,
      totalCapital: 134000,
      avgEntryPrice: 0.00000234,
      bestROI: 89.2,
      worstROI: 15,
      firstDetection: '2026-02-25T05:15:00Z',
      consensusType: 'EARLY STAGE',
      detectionDate: '2026-02-25 05:15 UTC',
      intervalMin: '0m 48s',
      intervalMax: '8m 31s',
    },
    wallets: [
      { address: '0xhi67...jk8', qualityScore: 80, winRate: 79, roi: 89.2, entryPrice: 0.00000228, positionSize: 52000 },
      { address: '0xlm90...no1', qualityScore: 73, winRate: 72, roi: 54.6, entryPrice: 0.00000236, positionSize: 44000 },
      { address: '0xpq23...rs4', qualityScore: 65, winRate: 64, roi: 15.0, entryPrice: 0.00000240, positionSize: 38000 },
    ],
    performance7d: [
      { date: '2026-02-18', value: 100 },
      { date: '2026-02-19', value: 102 },
      { date: '2026-02-20', value: 106 },
      { date: '2026-02-21', value: 108 },
      { date: '2026-02-22', value: 112 },
      { date: '2026-02-23', value: 110 },
      { date: '2026-02-24', value: 116 },
    ],
    detectedAt: '12h ago',
  },
];

export function getWalletDetails(address: string, tokenSymbol: string): WalletDetails {
  const walletDetailsMap: Record<string, WalletDetails> = {
    '0x7a3b...f56': {
      address: '0x7a3b...f56',
      qualityScore: 92,
      winRate: 91,
      roi: 234.7,
      metrics: {
        totalInvested: 312000,
        totalRealized: 1043000,
        currentHoldings: 218000,
        totalTrades: 58,
        winningTrades: 53,
        losingTrades: 5,
        avgWin: 892,
        avgLoss: 234,
        bestTrade: 2100,
        worstTrade: -389,
      },
      currentPosition: {
        token: tokenSymbol,
        entryPrice: 0.0085,
        currentPrice: 0.0285,
        size: 68000,
        unrealizedPnl: 135200,
        entryDate: '2026-02-23',
        holdingTime: '2d 6h',
      },
      interactions: {
        buyWallets: [
          { address: '0xee6b...b30', txCount: 15, volume: 312000, role: 'Accumulation' },
          { address: '0xff7a...c43', txCount: 10, volume: 198000, role: 'Co-buyer' },
          { address: '0x118d...d56', txCount: 7, volume: 112000, role: 'Co-buyer' },
        ],
        dumpWallets: [
          { address: '0xddc5...a17', txCount: 4, volume: 89000, role: 'Distribution' },
          { address: '0x8fe3...b21', txCount: 3, volume: 56000, role: 'Distribution' },
        ],
        cex: [
          { address: '0xccd4...e91', name: 'Binance', txCount: 2, volume: 24000 },
        ],
        unknown: [
          { address: '0xabc1...def', txCount: 3, volume: 12000 },
          { address: '0x456g...hij', txCount: 1, volume: 5000 },
        ],
      },
    },
    '0x2e1c...a89': {
      address: '0x2e1c...a89',
      qualityScore: 88,
      winRate: 85,
      roi: 187.3,
      metrics: {
        totalInvested: 245000,
        totalRealized: 704000,
        currentHoldings: 156000,
        totalTrades: 42,
        winningTrades: 36,
        losingTrades: 6,
        avgWin: 734,
        avgLoss: 166,
        bestTrade: 1200,
        worstTrade: -259,
      },
      currentPosition: {
        token: tokenSymbol,
        entryPrice: 0.0089,
        currentPrice: 0.0256,
        size: 45000,
        unrealizedPnl: 84000,
        entryDate: '2026-02-24',
        holdingTime: '1d 4h',
      },
      interactions: {
        buyWallets: [
          { address: '0xee6b...b30', txCount: 12, volume: 245000, role: 'Accumulation' },
          { address: '0xff7a...c43', txCount: 8, volume: 156000, role: 'Co-buyer' },
          { address: '0x118d...d56', txCount: 5, volume: 89000, role: 'Co-buyer' },
        ],
        dumpWallets: [
          { address: '0xddc5...a17', txCount: 3, volume: 67000, role: 'Distribution' },
          { address: '0x8fe3...b21', txCount: 2, volume: 45000, role: 'Distribution' },
        ],
        cex: [
          { address: '0xccd4...e91', name: 'Binance', txCount: 1, volume: 12000 },
        ],
        unknown: [
          { address: '0xabc1...def', txCount: 2, volume: 8000 },
          { address: '0x456g...hij', txCount: 1, volume: 3000 },
        ],
      },
    },
  };

  return walletDetailsMap[address] || {
    address,
    qualityScore: Math.floor(Math.random() * 30) + 65,
    winRate: Math.floor(Math.random() * 25) + 65,
    roi: Math.floor(Math.random() * 150) + 30,
    metrics: {
      totalInvested: Math.floor(Math.random() * 200000) + 80000,
      totalRealized: Math.floor(Math.random() * 500000) + 150000,
      currentHoldings: Math.floor(Math.random() * 150000) + 50000,
      totalTrades: Math.floor(Math.random() * 40) + 15,
      winningTrades: Math.floor(Math.random() * 30) + 10,
      losingTrades: Math.floor(Math.random() * 10) + 2,
      avgWin: Math.floor(Math.random() * 500) + 300,
      avgLoss: Math.floor(Math.random() * 200) + 80,
      bestTrade: Math.floor(Math.random() * 1500) + 500,
      worstTrade: -(Math.floor(Math.random() * 300) + 100),
    },
    currentPosition: {
      token: tokenSymbol,
      entryPrice: 0.0089,
      currentPrice: 0.0256,
      size: Math.floor(Math.random() * 60000) + 20000,
      unrealizedPnl: Math.floor(Math.random() * 80000) + 10000,
      entryDate: '2026-02-24',
      holdingTime: '1d 2h',
    },
    interactions: {
      buyWallets: [
        { address: '0xee6b...b30', txCount: 8, volume: 145000, role: 'Co-buyer' },
        { address: '0xff7a...c43', txCount: 5, volume: 98000, role: 'Co-buyer' },
      ],
      dumpWallets: [
        { address: '0xddc5...a17', txCount: 2, volume: 45000, role: 'Distribution' },
      ],
      cex: [
        { address: '0xccd4...e91', name: 'Coinbase', txCount: 1, volume: 8000 },
      ],
      unknown: [
        { address: '0xabc1...def', txCount: 1, volume: 4000 },
      ],
    },
  };
}

export function getNetworkData(wallet: WalletDetails) {
  const nodes: NetworkNode[] = [
    { id: wallet.address, type: 'smart', address: wallet.address, volume: wallet.metrics.totalInvested },
  ];
  const links: NetworkLink[] = [];

  wallet.interactions.buyWallets.forEach((w) => {
    nodes.push({ id: w.address, type: 'buy', address: w.address, volume: w.volume });
    links.push({ source: wallet.address, target: w.address, txCount: w.txCount, volume: w.volume });
  });

  wallet.interactions.dumpWallets.forEach((w) => {
    nodes.push({ id: w.address, type: 'dump', address: w.address, volume: w.volume });
    links.push({ source: wallet.address, target: w.address, txCount: w.txCount, volume: w.volume });
  });

  wallet.interactions.cex.forEach((w) => {
    nodes.push({ id: w.address, type: 'cex', address: w.address, volume: w.volume });
    links.push({ source: wallet.address, target: w.address, txCount: w.txCount, volume: w.volume });
  });

  wallet.interactions.unknown.forEach((w) => {
    nodes.push({ id: w.address, type: 'unknown', address: w.address, volume: w.volume });
    links.push({ source: wallet.address, target: w.address, txCount: w.txCount, volume: w.volume });
  });

  return { nodes, links };
}
