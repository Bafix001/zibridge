"use client";

import React, { useState } from "react";
import { 
  Typography, Card, Row, Col, Button, Modal, 
  message, Space, Tag, Progress, Empty, theme 
} from "antd";
import { 
  RollbackOutlined, DatabaseOutlined,
  TeamOutlined, ShopOutlined,
  FileTextOutlined, ExclamationCircleOutlined,
  TagOutlined, SafetyCertificateOutlined, InfoCircleOutlined
} from "@ant-design/icons";
import dayjs from "dayjs";

const { Title, Text } = Typography;

interface SnapshotRecord {
  id: number;
  created_at: string;
  source_name: string;
  status: string;
  total_objects: number;
  detected_entities?: Record<string, number>;
}

export default function SnapshotShowView({ record }: { record: SnapshotRecord }) {
  const { token } = theme.useToken();
  const [restoreModalOpen, setRestoreModalOpen] = useState(false);
  const [restoring, setRestoring] = useState(false);

  if (!record) return <Empty description="Données indisponibles" />;

  const isCompleted = record.status === "completed" && record.total_objects > 0;
  const snapshotDate = dayjs(record.created_at);

  const handleRestore = async () => {
    setRestoring(true);
    try {
      const response = await fetch(`http://localhost:8000/api/restore/${record.id}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ selective: false, dry_run: false, crm_type: "hubspot" }) 
      });
      if (response.ok) {
        message.success('✅ Restauration réussie !');
        setRestoreModalOpen(false);
      } else {
        message.error('❌ Erreur lors de la procédure.');
      }
    } catch (error) {
      message.error('❌ Serveur injoignable.');
    } finally {
      setRestoring(false);
    }
  };

  return (
    <div style={{ background: token.colorBgLayout, padding: '4px' }}>
      {/* Bandeau d'état - Correction Dark Mode */}
      <Card 
        variant="borderless" 
        style={{ 
          marginBottom: 24, 
          borderRadius: 16,
          background: token.colorBgContainer,
          borderLeft: `6px solid ${isCompleted ? token.colorSuccess : token.colorError}`,
        }}
      >
        <Row gutter={[24, 24]} align="middle">
          <Col xs={24} sm={6}>
            <Space direction="vertical" size={0}>
              <Text type="secondary" style={{ fontSize: '10px', fontWeight: 'bold' }}>HORODATAGE</Text>
              <Text strong>{snapshotDate.format("DD MMM YYYY • HH:mm")}</Text>
            </Space>
          </Col>
          <Col xs={24} sm={6}>
            <Space direction="vertical" size={0}>
              <Text type="secondary" style={{ fontSize: '10px', fontWeight: 'bold' }}>SOURCE</Text>
              <Text strong>{record.source_name?.toUpperCase()}</Text>
            </Space>
          </Col>
          <Col xs={24} sm={6}>
            <Space direction="vertical" size={0}>
              <Text type="secondary" style={{ fontSize: '10px', fontWeight: 'bold' }}>VOLUME TOTAL</Text>
              <Text strong style={{ fontSize: '18px' }}>{record.total_objects?.toLocaleString()}</Text>
            </Space>
          </Col>
          <Col xs={24} sm={6} style={{ textAlign: 'right' }}>
            <Button
              danger
              icon={<RollbackOutlined />}
              disabled={!isCompleted}
              onClick={() => setRestoreModalOpen(true)}
            >
              Restaurer
            </Button>
          </Col>
        </Row>
      </Card>

      <Title level={5} style={{ marginBottom: 16 }}><InfoCircleOutlined /> Composition du Graphe</Title>
      
      <Row gutter={[16, 16]}>
        {[
          { label: 'Companies', key: 'company', icon: <ShopOutlined />, color: '#1890ff' },
          { label: 'Contacts', key: 'contact', icon: <TeamOutlined />, color: '#722ed1' },
          { label: 'Deals', key: 'deal', icon: <FileTextOutlined />, color: '#fa8c16' },
          { label: 'Tickets', key: 'ticket', icon: <TagOutlined />, color: '#eb2f96' },
        ].map((type) => {
          const count = record.detected_entities?.[type.key] ?? 0;
          const percent = record.total_objects > 0 ? (count / record.total_objects) * 100 : 0;
          return (
            <Col xs={24} md={6} key={type.key}>
              <Card variant="borderless" style={{ borderRadius: 12, background: token.colorBgContainer }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                   <Text type="secondary">{type.label}</Text>
                   <Text strong>{count}</Text>
                </div>
                <Progress percent={percent} showInfo={false} strokeColor={type.color} size="small" />
              </Card>
            </Col>
          );
        })}
      </Row>

      <Modal
        title="RESTAURATION SYSTÈME"
        open={restoreModalOpen}
        onOk={handleRestore}
        confirmLoading={restoring}
        onCancel={() => setRestoreModalOpen(false)}
        okText="Lancer"
        okButtonProps={{ danger: true }}
        centered
      >
        <Text>Confirmez-vous l'écrasement des données actuelles par ce snapshot ?</Text>
      </Modal>
    </div>
  );
}