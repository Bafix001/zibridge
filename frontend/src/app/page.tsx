"use client";

import React, { useState } from "react";
import { 
  Card, Tabs, Button, Space, Typography, Input, 
  theme, Empty, Row, Col, Badge, Tag, message, Modal, Form, Progress
} from "antd";
import { 
  PlusOutlined, ApiOutlined, FileTextOutlined, 
  MergeCellsOutlined, DatabaseOutlined, RocketOutlined,
  ExperimentOutlined, GlobalOutlined, KeyOutlined, ExclamationCircleOutlined
} from "@ant-design/icons";

import SnapshotList from "@/components/SnapshotList/SnapshotList";

const { Title, Text, Paragraph } = Typography;
const { confirm } = Modal;

interface SourceInstance {
  key: string;
  label: string;
  type: "api" | "file" | "none";
  providerName?: string;
  status: "idle" | "syncing" | "connected";
  progress?: number;
}

export default function DashboardPage() {
  const { token: themeToken } = theme.useToken();
  const [sources, setSources] = useState<SourceInstance[]>([
    { key: "1", label: "Session Initiale", type: "none", status: "idle" }
  ]);
  const [activeKey, setActiveKey] = useState("1");
  const [fileToUpload, setFileToUpload] = useState<File | null>(null);

  // --- GESTION DES ONGLETS AVEC CONFIRMATION ---
  
  const addSource = () => {
    const newKey = String(Date.now());
    setSources([...sources, { key: newKey, label: `New Source`, type: "none", status: "idle" }]);
    setActiveKey(newKey);
  };

  const removeSource = (targetKey: string) => {
    confirm({
      title: 'Voulez-vous fermer cet onglet ?',
      icon: <ExclamationCircleOutlined style={{ color: '#ff4d4f' }} />,
      content: 'Toutes les configurations non sauvegardées pour cette source seront perdues.',
      okText: 'Fermer',
      okType: 'danger',
      cancelText: 'Annuler',
      onOk() {
        const newSources = sources.filter(s => s.key !== targetKey);
        setSources(newSources);
        if (newSources.length && activeKey === targetKey) {
          setActiveKey(newSources[newSources.length - 1].key);
        }
      },
    });
  };

  // --- ENVOI DU CSV AU BACKEND ---

  const handleIngestion = async (sourceKey: string) => {
    const source = sources.find(s => s.key === sourceKey);
    
    if (source?.type === "file" && fileToUpload) {
      const formData = new FormData();
      formData.append("file", fileToUpload);
      formData.append("provider", source.providerName || "Agnostic_CSV");

      updateSource(sourceKey, { status: "syncing", progress: 30 });

      try {
        const response = await fetch("http://localhost:8000/api/sync/upload", {
          method: "POST",
          body: formData,
        });

        if (response.ok) {
          updateSource(sourceKey, { status: "connected", progress: 100 });
          message.success(`SNAP-00${Math.floor(Math.random() * 99)} créé avec succès !`);
        } else {
          throw new Error();
        }
      } catch (err) {
        message.error("Erreur lors de l'envoi au backend FastAPI.");
        updateSource(sourceKey, { status: "idle" });
      }
    } else {
      // Simulation pour API
      updateSource(sourceKey, { status: "connected" });
    }
  };

  const updateSource = (key: string, data: Partial<SourceInstance>) => {
    setSources(prev => prev.map(s => s.key === key ? { ...s, ...data } : s));
  };

  const renderContent = (source: SourceInstance) => {
    if (source.type === "none") {
      return (
        <Card variant="borderless" style={{ textAlign: "center", padding: "80px 0" }}>
          <ExperimentOutlined style={{ fontSize: 48, color: themeToken.colorPrimary }} />
          <Title level={3} style={{ marginTop: 20 }}>Démarrer une manipulation</Title>
          <Space size="large" style={{ marginTop: 30 }}>
            <Button size="large" icon={<GlobalOutlined />} onClick={() => updateSource(source.key, { type: "api" })}>API Agnostique</Button>
            <Button size="large" icon={<FileTextOutlined />} onClick={() => updateSource(source.key, { type: "file" })}>Fichier CSV</Button>
          </Space>
        </Card>
      );
    }

    return (
      <Row gutter={[24, 24]}>
        <Col span={source.status === "connected" ? 0 : 10}>
          <Card title="Configuration" variant="borderless">
            <Form layout="vertical" onFinish={() => handleIngestion(source.key)}>
              <Form.Item label="Nom de la source">
                <Input placeholder="Ex: Clients_Export_Dec" onChange={(e) => updateSource(source.key, { providerName: e.target.value, label: e.target.value })} />
              </Form.Item>
              
              {source.type === "api" ? (
                <Form.Item label="Endpoint URL">
                  <Input placeholder="https://api.service.com/v1" prefix={<GlobalOutlined />} />
                </Form.Item>
              ) : (
                <Form.Item label="Fichier CSV">
                  <Input type="file" accept=".csv" onChange={(e) => setFileToUpload(e.target.files?.[0] || null)} />
                </Form.Item>
              )}

              <Button 
                type="primary" 
                block 
                size="large" 
                icon={<RocketOutlined />} 
                htmlType="submit" 
                loading={source.status === "syncing"}
              >
                Lancer l'Ingestion
              </Button>
            </Form>
          </Card>
        </Col>

        <Col span={source.status === "connected" ? 24 : 14}>
          {source.status === "syncing" ? (
            <div style={{ textAlign: 'center', padding: 100 }}>
              <Progress type="circle" percent={source.progress} />
              <div style={{ marginTop: 20 }}><Text strong>Traitement par le moteur FastAPI...</Text></div>
            </div>
          ) : source.status === "connected" ? (
            <SnapshotList /> 
          ) : (
            <Empty description="Prêt pour l'ingestion" style={{ padding: 60 }} />
          )}
        </Col>
      </Row>
    );
  };

  return (
    <div style={{ padding: "0 24px" }}>
      <Row justify="space-between" align="middle" style={{ margin: "24px 0" }}>
        <Col><Title level={2}>ZIBRIDGE <Tag color="cyan">UNIVERSAL</Tag></Title></Col>
        <Col><Button icon={<MergeCellsOutlined />} size="large">Fusionner les sources</Button></Col>
      </Row>

      <Tabs
        type="editable-card"
        activeKey={activeKey}
        onChange={setActiveKey}
        onEdit={(key, action) => action === 'add' ? addSource() : removeSource(key as string)}
        items={sources.map(s => ({
          key: s.key,
          label: (
            <span>
              <Badge status={s.status === "connected" ? "success" : "default"} dot style={{ marginRight: 8 }} />
              {s.label}
            </span>
          ),
          children: renderContent(s),
          closable: true
        }))}
      />
    </div>
  );
}