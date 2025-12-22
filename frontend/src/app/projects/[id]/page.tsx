"use client";

import React, { useState, useMemo } from "react";
import { useParams } from "next/navigation";
import {
  Typography, Card, Statistic, Row, Col, theme, Space, Button,
  Modal, Input, Upload, message, Tooltip
} from "antd";
import {
  DatabaseOutlined, HistoryOutlined, FolderOutlined,
  UploadOutlined, ReloadOutlined, CheckCircleOutlined,
  SyncOutlined, PlayCircleOutlined
} from "@ant-design/icons";
import axios from "axios";
import useSWR from "swr";
import SnapshotList from "../../../components/SnapshotList/SnapshotList";

const { Title, Text } = Typography;

const fetcher = (url: string) => axios.get(`http://localhost:8000${url}`).then(res => res.data);

export default function ProjectDetailsPage() {
  const params = useParams();
  const id = useMemo(() => (Array.isArray(params?.id) ? params.id[0] : (params?.id as string)), [params?.id]);
  const { token } = theme.useToken();

  const [isRunModalOpen, setIsRunModalOpen] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false);
  const [fileList, setFileList] = useState<any[]>([]);
  const [apiToken, setApiToken] = useState("");

  // Charger le projet
  const { data: project, mutate: mutateProject } = useSWR(
    id ? `/projects/${id}` : null, 
    fetcher
  );
  
  // Charger les snapshots
  const { data: snapshots, mutate: mutateSnapshots } = useSWR(
    id ? `/projects/${id}/snapshots` : null, 
    fetcher,
    { refreshInterval: 3000 }
  );

  const projectName = project?.name || `Projet #${id}`;
  const snapshotsList = Array.isArray(snapshots) ? snapshots : [];
  const latestSnapshot = snapshotsList[0];
  const totalSnapshots = snapshotsList.length;
  const totalObjects = latestSnapshot?.total_objects || 0;
  const isRunning = latestSnapshot?.status === "running";
  
  // 🔥 DÉTECTION DU TYPE DE SOURCE
  const sourceType = project?.config?.source_type || project?.default_source_type;
  const isFileProject = sourceType === "file";
  const provider = project?.config?.provider || "unknown";
  const hasToken = !!project?.config?.token;

  // 🔥 BOUTON RUN PRINCIPAL
  const handleRun = () => {
    setIsRunModalOpen(true);
  };

  // 📁 Upload fichier
  const handleFileRun = async () => {
    if (fileList.length === 0) {
      message.error("Fichier manquant");
      return;
    }

    try {
      setIsProcessing(true);
      const formData = new FormData();
      formData.append("file", fileList[0]);
      formData.append("project_id", id);
      
      await axios.post("http://localhost:8000/api/sync/upload", formData);
      
      message.success("✅ Analyse lancée !");
      setIsRunModalOpen(false);
      setFileList([]);
      mutateSnapshots();
    } catch (error) {
      message.error("❌ Erreur moteur");
    } finally {
      setIsProcessing(false);
    }
  };

  // ☁️ Config/Run API
  const handleApiRun = async () => {
    if (!apiToken.trim()) {
      message.error("Token requis");
      return;
    }

    try {
      setIsProcessing(true);
      
      // Sauvegarder le token
      if (!hasToken) {
        await axios.patch(`http://localhost:8000/projects/${id}`, {
          config: {
            ...project?.config,
            token: apiToken
          }
        });
      }

      // Lancer la sync
      await axios.post("http://localhost:8000/api/sync/start", {
        crm_type: provider,
        credentials: { token: apiToken },
        project_id: parseInt(id, 10),
      });

      message.success("✅ Synchronisation lancée !");
      setIsRunModalOpen(false);
      setApiToken("");
      mutateProject();
      mutateSnapshots();
    } catch (error) {
      message.error("❌ Erreur");
    } finally {
      setIsProcessing(false);
    }
  };

  return (
    <div style={{ padding: "24px", background: token.colorBgLayout, minHeight: "100vh" }}>
      {/* HEADER */}
      <Card variant="borderless" style={{ marginBottom: 24, borderRadius: 12 }}>
        <Row justify="space-between" align="middle">
          <Col>
            <Space size="middle">
              <div style={{ background: token.colorPrimaryBg, padding: 12, borderRadius: 12 }}>
                <FolderOutlined style={{ fontSize: 24, color: token.colorPrimary }} />
              </div>
              <div>
                <Title level={4} style={{ margin: 0 }}>{projectName}</Title>
                <Text type="secondary" style={{ fontSize: 12 }}>
                  {isFileProject ? "Fichier CSV" : provider.toUpperCase()}
                </Text>
              </div>
            </Space>
          </Col>
          <Col>
            <Space>
              <Tooltip title="Actualiser">
                <Button 
                  type="text" 
                  icon={<ReloadOutlined />} 
                  onClick={() => mutateSnapshots()}
                />
              </Tooltip>
              <Button
                type="primary"
                size="large"
                icon={<PlayCircleOutlined />}
                onClick={handleRun}
                loading={isRunning || isProcessing}
                disabled={isRunning}
              >
                RUN
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* STATS */}
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col span={8}>
          <Card variant="borderless" style={{ borderRadius: 12 }}>
            <Statistic title="HISTORIQUE" value={totalSnapshots} prefix={<HistoryOutlined />} />
          </Card>
        </Col>
        <Col span={8}>
          <Card variant="borderless" style={{ borderRadius: 12 }}>
            <Statistic title="OBJETS" value={totalObjects} prefix={<DatabaseOutlined />} />
          </Card>
        </Col>
        <Col span={8}>
          <Card variant="borderless" style={{ borderRadius: 12 }}>
            <Statistic
              title="ÉTAT"
              value={isRunning ? "EN COURS" : "PRÊT"}
              valueStyle={{ color: isRunning ? token.colorWarning : token.colorSuccess }}
              prefix={isRunning ? <SyncOutlined spin /> : <CheckCircleOutlined />}
            />
          </Card>
        </Col>
      </Row>

      {/* LISTE */}
      <Card
        title={<Space><DatabaseOutlined /><span>Historique des versions</span></Space>}
        variant="borderless"
        style={{ borderRadius: 12 }}
      >
        <SnapshotList projectId={id} />
      </Card>

      {/* MODAL RUN */}
      <Modal
        title={
          isFileProject 
            ? "📤 Upload & Run" 
            : hasToken 
              ? `☁️ Re-sync ${provider.toUpperCase()}`
              : `🔑 Configurer ${provider.toUpperCase()}`
        }
        open={isRunModalOpen}
        onCancel={() => !isProcessing && setIsRunModalOpen(false)}
        footer={null}
        centered
        destroyOnClose
      >
        {isFileProject ? (
          // 📁 MODE FICHIER
          <Space direction="vertical" style={{ width: "100%" }} size="middle">
            <Text type="secondary">
              Glissez votre fichier CSV pour créer un nouveau snapshot
            </Text>
            <Upload.Dragger
              multiple={false}
              beforeUpload={(file) => {
                setFileList([file]);
                return false;
              }}
              fileList={fileList}
              onRemove={() => setFileList([])}
              disabled={isProcessing}
            >
              <p className="ant-upload-drag-icon">
                <UploadOutlined style={{ color: token.colorPrimary, fontSize: 48 }} />
              </p>
              <p className="ant-upload-text">Cliquez ou glissez votre fichier</p>
              <p className="ant-upload-hint">Format accepté : CSV</p>
            </Upload.Dragger>
            <Button
              type="primary"
              block
              size="large"
              onClick={handleFileRun}
              loading={isProcessing}
              disabled={fileList.length === 0}
              icon={<PlayCircleOutlined />}
            >
              LANCER L'ANALYSE
            </Button>
          </Space>
        ) : (
          // ☁️ MODE API
          <Space direction="vertical" style={{ width: "100%" }} size="middle">
            {hasToken ? (
              <>
                <Text type="secondary">
                  Token configuré. Lancer une nouvelle synchronisation ?
                </Text>
                <Button
                  type="primary"
                  block
                  size="large"
                  onClick={async () => {
                    try {
                      setIsProcessing(true);
                      await axios.post("http://localhost:8000/api/sync/start", {
                        crm_type: provider,
                        credentials: { token: project?.config?.token },
                        project_id: parseInt(id, 10),
                      });
                      message.success("✅ Synchronisation lancée !");
                      setIsRunModalOpen(false);
                      mutateSnapshots();
                    } catch (error) {
                      message.error("❌ Erreur");
                    } finally {
                      setIsProcessing(false);
                    }
                  }}
                  loading={isProcessing}
                  icon={<PlayCircleOutlined />}
                >
                  SYNCHRONISER
                </Button>
              </>
            ) : (
              <>
                <Text type="secondary">
                  Entrez votre token API
                </Text>
                <Input.Password
                  size="large"
                  placeholder={`Token ${provider.toUpperCase()}`}
                  value={apiToken}
                  onChange={(e) => setApiToken(e.target.value)}
                  disabled={isProcessing}
                />
                <Button
                  type="primary"
                  block
                  size="large"
                  onClick={handleApiRun}
                  loading={isProcessing}
                  icon={<PlayCircleOutlined />}
                >
                  CONFIGURER & LANCER
                </Button>
              </>
            )}
          </Space>
        )}
      </Modal>
    </div>
  );
}