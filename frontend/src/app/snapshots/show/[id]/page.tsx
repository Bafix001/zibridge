"use client";

import { Show } from "@refinedev/antd";
import { useShow, useInvalidate } from "@refinedev/core";
import { 
  Typography, 
  Card, 
  Row, 
  Col, 
  Statistic, 
  Button, 
  Modal, 
  message, 
  Space, 
  Tag, 
  Divider,
  Progress 
} from "antd";
import { 
  RollbackOutlined, 
  DatabaseOutlined, 
  ClockCircleOutlined,
  CheckCircleOutlined,
  TeamOutlined,
  ShopOutlined,
  FileTextOutlined
} from "@ant-design/icons";
import { useState } from "react";

const { Title, Text } = Typography;

export default function SnapshotShow() {
  const { query } = useShow({});
  const invalidate = useInvalidate();
  const { data, isLoading } = query;
  const record = data?.data;

  const [restoreModalOpen, setRestoreModalOpen] = useState(false);
  const [restoring, setRestoring] = useState(false);

  const handleRestore = async () => {
    setRestoring(true);
    
    try {
      const response = await fetch(`http://localhost:8000/restore/${record?.id}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
      });

      if (response.ok) {
        message.success('✅ Restauration réussie !');
        setRestoreModalOpen(false);
        invalidate({
          resource: "snapshots",
          invalidates: ["all"],
        });
      } else {
        message.error('❌ Erreur lors de la restauration');
      }
    } catch (error) {
      message.error('❌ Erreur de connexion');
    } finally {
      setRestoring(false);
    }
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString('fr-FR', {
      day: '2-digit',
      month: 'long',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  };

  const getStatusColor = (status: string) => {
    return status === 'completed' ? 'success' : 'warning';
  };

  const getTypeIcon = (type: string) => {
    const icons: Record<string, any> = {
      companies: <ShopOutlined style={{ fontSize: 24 }} />,
      contacts: <TeamOutlined style={{ fontSize: 24 }} />,
      deals: <FileTextOutlined style={{ fontSize: 24 }} />
    };
    return icons[type] || <DatabaseOutlined style={{ fontSize: 24 }} />;
  };

  const totalObjects = record?.item_count || 0;
  const itemsByType = record?.items_by_type || {};

  return (
    <Show
      isLoading={isLoading}
      title={
        <Space direction="vertical" size={0}>
          <Text type="secondary" style={{ fontSize: 14 }}>Snapshot</Text>
          <Title level={3} style={{ margin: 0 }}>
            #{record?.id}
          </Title>
        </Space>
      }
      headerButtons={({ defaultButtons }) => (
        <>
          {defaultButtons}
          <Button
            type="primary"
            icon={<RollbackOutlined />}
            onClick={() => setRestoreModalOpen(true)}
            danger
            size="large"
          >
            Restaurer
          </Button>
        </>
      )}
    >
      {/* Header Info Card */}
      <Card 
        bordered={false}
        style={{ 
          marginBottom: 24,
          background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
          color: 'white'
        }}
      >
        <Row gutter={[24, 24]}>
          <Col xs={24} md={12}>
            <Space direction="vertical" size={4}>
              <Space>
                <ClockCircleOutlined style={{ fontSize: 20 }} />
                <Text style={{ color: 'rgba(255,255,255,0.8)' }}>Date de création</Text>
              </Space>
              <Title level={4} style={{ color: 'white', margin: 0 }}>
                {record?.timestamp && formatDate(record.timestamp)}
              </Title>
            </Space>
          </Col>
          <Col xs={24} md={12}>
            <Space direction="vertical" size={4}>
              <Space>
                <DatabaseOutlined style={{ fontSize: 20 }} />
                <Text style={{ color: 'rgba(255,255,255,0.8)' }}>Source</Text>
              </Space>
              <Title level={4} style={{ color: 'white', margin: 0 }}>
                {record?.source}
              </Title>
            </Space>
          </Col>
        </Row>
        
        <Divider style={{ borderColor: 'rgba(255,255,255,0.2)', margin: '20px 0' }} />
        
        <Row align="middle" justify="space-between">
          <Col>
            <Space size="large">
              <div>
                <Text style={{ color: 'rgba(255,255,255,0.8)', fontSize: 12 }}>
                  STATUT
                </Text>
                <div style={{ marginTop: 4 }}>
                  <Tag 
                    color={getStatusColor(record?.status)} 
                    style={{ borderRadius: 12, padding: '4px 12px' }}
                  >
                    {record?.status === 'completed' ? (
                      <><CheckCircleOutlined /> Complété</>
                    ) : (
                      <><ClockCircleOutlined /> En attente</>
                    )}
                  </Tag>
                </div>
              </div>
              <div>
                <Text style={{ color: 'rgba(255,255,255,0.8)', fontSize: 12 }}>
                  TOTAL OBJETS
                </Text>
                <Title level={3} style={{ color: 'white', margin: '4px 0 0 0' }}>
                  {totalObjects.toLocaleString()}
                </Title>
              </div>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* Statistics Cards */}
      <Row gutter={[16, 16]}>
        {Object.entries(itemsByType).map(([type, count]) => {
          const percentage = ((count as number) / totalObjects) * 100;
          
          return (
            <Col xs={24} sm={12} lg={8} key={type}>
              <Card 
                hoverable
                bordered={false}
                style={{ 
                  borderRadius: 12,
                  boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
                  height: '100%'
                }}
              >
                <Space direction="vertical" size={16} style={{ width: '100%' }}>
                  <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }}>
                    <Space>
                      <div style={{ 
                        background: '#f0f5ff', 
                        padding: 12, 
                        borderRadius: 8,
                        color: '#1890ff'
                      }}>
                        {getTypeIcon(type)}
                      </div>
                      <div>
                        <Text type="secondary" style={{ fontSize: 12 }}>
                          {type.toUpperCase()}
                        </Text>
                        <Title level={2} style={{ margin: 0 }}>
                          {(count as number).toLocaleString()}
                        </Title>
                      </div>
                    </Space>
                  </Space>
                  
                  <div>
                    <div style={{ marginBottom: 8, display: 'flex', justifyContent: 'space-between' }}>
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        Progression
                      </Text>
                      <Text strong style={{ fontSize: 12 }}>
                        {percentage.toFixed(1)}%
                      </Text>
                    </div>
                    <Progress 
                      percent={percentage} 
                      showInfo={false}
                      strokeColor={{
                        '0%': '#667eea',
                        '100%': '#764ba2',
                      }}
                    />
                  </div>
                </Space>
              </Card>
            </Col>
          );
        })}
      </Row>

      {/* Restore Modal */}
      <Modal
        title={
          <Space>
            <RollbackOutlined style={{ color: '#ff4d4f' }} />
            <span>Confirmer la restauration</span>
          </Space>
        }
        open={restoreModalOpen}
        onOk={handleRestore}
        onCancel={() => setRestoreModalOpen(false)}
        confirmLoading={restoring}
        okText="Restaurer maintenant"
        cancelText="Annuler"
        okButtonProps={{ danger: true, size: 'large' }}
        cancelButtonProps={{ size: 'large' }}
        width={600}
      >
        <Divider />
        <Space direction="vertical" size={16} style={{ width: '100%' }}>
          <Card 
            size="small" 
            style={{ background: '#fff7e6', border: '1px solid #ffd591' }}
          >
            <Space direction="vertical" size={8}>
              <Text strong>📸 Snapshot #{record?.id}</Text>
              <Text type="secondary">
                Créé le {record?.timestamp && formatDate(record.timestamp)}
              </Text>
              <Divider style={{ margin: '8px 0' }} />
              <Text>
                <strong>{totalObjects.toLocaleString()}</strong> objets seront restaurés
              </Text>
            </Space>
          </Card>

          <Card 
            size="small" 
            style={{ background: '#fff1f0', border: '1px solid #ffccc7' }}
          >
            <Space direction="vertical" size={8}>
              <Text strong style={{ color: '#ff4d4f' }}>⚠️ Mode Sélectif</Text>
              <Text type="secondary">
                Seuls les objets <strong>modifiés</strong> ou <strong>supprimés</strong> depuis ce snapshot seront restaurés.
              </Text>
              <Text type="secondary">
                Les objets identiques seront ignorés.
              </Text>
            </Space>
          </Card>

          <Text type="secondary" style={{ fontSize: 12 }}>
            💡 L'Auto-Suture recréera automatiquement toutes les associations entre objets.
          </Text>
        </Space>
        <Divider />
      </Modal>
    </Show>
  );
}