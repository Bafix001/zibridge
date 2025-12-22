"use client";

import React from "react";
import { Space, Table, Tag, Button, Typography, theme } from "antd";
import {
  SyncOutlined, CheckCircleOutlined, ExclamationCircleOutlined,
  SwapOutlined, FileTextOutlined, CloudOutlined, EyeOutlined
} from "@ant-design/icons";
import { useRouter } from "next/navigation";
import dayjs from "dayjs";
import axios from "axios";
import useSWR from "swr";

const { Text } = Typography;

const fetcher = async (url: string) => {
  const fullUrl = `http://localhost:8000${url}`;
  const res = await axios.get(fullUrl);
  return res.data;
};

interface SnapshotListProps {
  projectId?: string | number;
}

interface ISnapshot {
  id: number;
  project_id?: number;
  created_at: string;
  source_name: string;
  status: string;
  total_objects: number;
  detected_entities?: Record<string, number>;
}

export default function SnapshotList({ projectId }: SnapshotListProps) {
  const router = useRouter();
  const { token } = theme.useToken();

  const { data: snapshots, error, isLoading } = useSWR<ISnapshot[]>(
    projectId ? `/projects/${projectId}/snapshots` : null,
    fetcher,
    { refreshInterval: 3000 }
  );

  const columns = [
    {
      dataIndex: "id",
      title: "RÉF.",
      width: 100,
      render: (val: number) => (
        <Tag color="cyan" style={{ fontFamily: 'monospace', fontSize: '12px', padding: '2px 8px' }}>
          SNAP-{String(val).padStart(3, '0')}
        </Tag>
      )
    },
    {
      dataIndex: "created_at",
      title: "DATE",
      width: 150,
      render: (val: string) => (
        <Text style={{ fontSize: '12px' }}>
          {dayjs(val).format("DD/MM HH:mm")}
        </Text>
      )
    },
    {
      dataIndex: "source_name",
      title: "SOURCE",
      width: 120,
      render: (val: string) => {
        const isFile = val?.toLowerCase() === "file";
        const displayName = isFile ? "FILE" : val?.toUpperCase();
        
        return (
          <Space>
            {isFile ? 
              <FileTextOutlined style={{ color: '#faad14' }} /> : 
              <CloudOutlined style={{ color: token.colorPrimary }} />
            }
            <Text style={{ fontSize: '12px' }}>{displayName}</Text>
          </Space>
        );
      }
    },
    {
      dataIndex: "status",
      title: "STATUT",
      width: 120,
      render: (val: string, record: ISnapshot) => {
        const effectiveStatus = (val === 'completed' && record.total_objects === 0) ? 'failed' : val;

        const statusConfig: Record<string, { color: string; icon: React.ReactNode; label: string }> = {
          completed: { color: "success", icon: <CheckCircleOutlined />, label: "TERMINÉ" },
          running: { color: "processing", icon: <SyncOutlined spin />, label: "EN COURS" },
          failed: { color: "error", icon: <ExclamationCircleOutlined />, label: "ÉCHEC" },
        };
        
        const config = statusConfig[effectiveStatus] || { 
          color: "default", 
          icon: null, 
          label: effectiveStatus 
        };
        
        return (
          <Tag color={config.color} icon={config.icon} style={{ fontSize: '11px' }}>
            {config.label}
          </Tag>
        );
      }
    },
    {
      dataIndex: "total_objects",
      title: "OBJETS",
      width: 100,
      align: "right" as const,
      render: (val: number) => <Text strong>{val?.toLocaleString() || 0}</Text>
    },
    {
      title: "ACTIONS",
      align: "right" as const,
      width: 120,
      render: (_: any, record: ISnapshot) => {
        const isFailed = record.status === "failed" || record.total_objects === 0;
        const isRunning = record.status === "running";
    
        // ❌ Si échec → Indisponible
        if (isFailed) {
          return <Text type="secondary" style={{ fontSize: 11 }}>Indisponible</Text>;
        }
    
        // ⏳ Si en cours → Désactivé
        if (isRunning) {
          return (
            <Space size="small">
              <Button size="small" type="text" icon={<EyeOutlined />} disabled />
              <Button size="small" type="text" icon={<SwapOutlined />} disabled />
            </Space>
          );
        }
    
        // ✅ Si succès → Voir + Diff
        return (
          <Space size="small">
            <Button 
              size="small" 
              type="text"
              icon={<EyeOutlined />}
              onClick={() => router.push(`/snapshots/show/${record.id}`)}
            />
            <Button 
              size="small" 
              type="text"
              icon={<SwapOutlined />} 
              onClick={() => router.push(`/diff?base=${record.id - 1}&target=${record.id}`)}
            />
          </Space>
        );
      }
    }
  ];

  if (error) return <Text type="danger">Erreur de chargement</Text>;

  return (
    <Table
      dataSource={snapshots || []}
      columns={columns}
      rowKey="id"
      pagination={{ pageSize: 10, showSizeChanger: false }}
      bordered={false}
      loading={isLoading}
      locale={{
        emptyText: "Aucun snapshot disponible"
      }}
    />
  );
}