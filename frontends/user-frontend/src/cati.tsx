import { createRoot } from 'react-dom/client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { CatiTrading } from './components/Dashboard/CatiTrading';
import './index.css';

const client = new QueryClient({ defaultOptions: { queries: { retry: 1 } } });
createRoot(document.getElementById('root')!).render(
  <QueryClientProvider client={client}>
    <main className="min-h-screen bg-slate-950 p-6"><div className="mx-auto max-w-7xl"><CatiTrading /></div></main>
  </QueryClientProvider>,
);
