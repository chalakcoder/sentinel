import { useState, useEffect, useRef, useCallback } from 'react';

export function useWebSocket(url) {
  const [events, setEvents]               = useState([]);
  const [connectionStatus, setStatus]     = useState('CONNECTING');
  const wsRef                             = useRef(null);
  const reconnectTimerRef                 = useRef(null);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    wsRef.current = new WebSocket(url);
    setStatus('CONNECTING');

    wsRef.current.onopen = () => {
      setStatus('CONNECTED');
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };

    wsRef.current.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data);
        if (data.type === 'ping') return;   // keepalive — ignore
        setEvents(prev => [data, ...prev].slice(0, 500));
      } catch {
        // ignore malformed frames
      }
    };

    wsRef.current.onclose = () => {
      setStatus('RECONNECTING');
      reconnectTimerRef.current = setTimeout(connect, 3000);
    };

    wsRef.current.onerror = () => {
      setStatus('ERROR');
      wsRef.current?.close();
    };
  }, [url]);

  useEffect(() => {
    connect();
    return () => {
      clearTimeout(reconnectTimerRef.current);
      wsRef.current?.close();
    };
  }, [connect]);

  return { events, connectionStatus };
}
