"use client";

import { useState, useEffect, Suspense } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import {
  Card, Select, Button, Space, Typography, Tag, Alert, Empty,
  Collapse, Row, Col, Statistic, message, Segmented, Pagination, Tooltip, theme, Skeleton
} from "antd";
import {
  SwapOutlined, PlusCircleOutlined, EditOutlined, DeleteOutlined,
  RollbackOutlined, ArrowRightOutlined, HistoryOutlined, SearchOutlined
} from "@ant-design/icons";
import { useList } from "@refinedev/core";
import dayjs from "dayjs";

const { Title, Text } = Typography;
const { Panel } = Collapse;
const PAGE_SIZE = 8;

// --- INTERFACES ---
interface Snapshot {
  id: number;
  timestamp: string;
  source: string;
  status: string;
}

interface DiffChange {
  old: any;
  new: any;
}

interface DiffItem {
  type: string;
  id: string | number;
  display_name?: string;
  changes?: Record<string, DiffChange>;
}

interface DiffResult {
  summary: { created: number; updated: number; deleted: number; };
  details: { created: DiffItem[]; updated: DiffItem[]; deleted: DiffItem[]; };
}

function DiffContent() {
  const { token } = theme.useToken();
  const router = useRouter();
  const searchParams = useSearchParams();
  
  const [baseSnapshot, setBaseSnapshot] = useState<number>();
  const [targetSnapshot, setTargetSnapshot] = useState<number>();
  const [diffResult, setDiffResult] = useState<DiffResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [objectFilter, setObjectFilter] = useState<string>("all");
  const [updatedPage, setUpdatedPage] = useState(1);

  const { result } = useList<Snapshot>({
    resource: "snapshots",
    pagination: { pageSize: 100 },
    sorters: [{ field: "id", order: "desc" }],
  });
  
  const snapshots: Snapshot[] = result?.data ?? [];
  

  useEffect(() => {
    const b = searchParams.get("base");
    const t = searchParams.get("target");
    if (b) setBaseSnapshot(Number(b));
    if (t) setTargetSnapshot(Number(t));
  }, [searchParams]);

  const handleCompare = async () => {
    if (!baseSnapshot || !targetSnapshot) return;
    setLoading(true);
    try {
      const response = await fetch(`http://localhost:8000/diff/${baseSnapshot}/${targetSnapshot}/details`);
      if (!response.ok) throw new Error();
      const data = await response.json();
      setDiffResult(data);
      message.success("Analyse comparative terminée");
    } catch (error) {
      message.error("Erreur d'analyse.");
    } finally {
      setLoading(false);
    }
  };

  const renderChanges = (changes: Record<string, DiffChange>) => (
    <div style={{ 
      background: token.colorFillAlter, 
      padding: "16px", 
      borderRadius: token.borderRadiusLG,
      border: `1px solid ${token.colorBorderSecondary}`
    }}>
      {Object.entries(changes).map(([key, value]) => {
        if (key.startsWith("_zibridge")) return null;
        return (
          <Row key={key} style={{ marginBottom: 8, paddingBottom: 8, borderBottom: `1px dashed ${token.colorBorderSecondary}` }} align="middle">
            <Col span={6}><Text code>{key}</Text></Col>
            <Col span={8}>
              <div style={{ color: token.colorError, background: token.colorErrorBg, padding: "2px 8px", borderRadius: 4, fontSize: "12px" }}>
                {String(value.old ?? "ø")}
              </div>
            </Col>
            <Col span={2} style={{ textAlign: "center" }}><ArrowRightOutlined style={{ opacity: 0.3 }} /></Col>
            <Col span={8}>
              <div style={{ color: token.colorSuccess, background: token.colorSuccessBg, padding: "2px 8px", borderRadius: 4, fontSize: "12px" }}>
                {String(value.new ?? "ø")}
              </div>
            </Col>
          </Row>
        );
      })}
    </div>
  );

  const filteredData = (type: keyof DiffResult['details']) => {
    if (!diffResult) return [];
    const list = diffResult.details[type];
    return objectFilter === "all" ? list : list.filter((i) => i.type === objectFilter);
  };

  const renderSnapshotLabel = (s: Snapshot) => (
    <Space>
      <Tag color="cyan" style={{ fontFamily: 'monospace' }}>SNAP-{String(s.id).padStart(4, '0')}</Tag>
      <Text type="secondary" style={{ fontSize: 12 }}> {dayjs(s.timestamp).format("DD/MM HH:mm")}</Text>

    </Space>
  );

  return (
    <div style={{ maxWidth: 1200, margin: "0 auto" }}>
      <div style={{ marginBottom: 32 }}>
        <Title level={2}><SwapOutlined /> Analyse Différentielle</Title>
        <Text type="secondary">Comparez deux points de contrôle pour auditer les changements de données.</Text>
      </div>

      <Card bordered={false} style={{ background: token.colorBgContainer, boxShadow: token.boxShadowTertiary, marginBottom: 24 }}>
        <Row gutter={24} align="middle">
          <Col span={10}>
            <Text strong style={{ fontSize: '11px', display: 'block', marginBottom: 8 }}>RÉFÉRENCE (ORIGINE)</Text>
            <Select 
              style={{ width: "100%" }} size="large" value={baseSnapshot} onChange={setBaseSnapshot}
              placeholder="Base version"
            >
              {snapshots.map((s: Snapshot) => <Select.Option key={s.id} value={s.id}>{renderSnapshotLabel(s)}</Select.Option>)}
            </Select>
          </Col>
          <Col span={4} style={{ textAlign: "center", paddingTop: 20 }}>
            <Button shape="circle" icon={<SwapOutlined />} onClick={() => {
              const t = baseSnapshot; setBaseSnapshot(targetSnapshot); setTargetSnapshot(t);
            }} />
          </Col>
          <Col span={10}>
            <Text strong style={{ fontSize: '11px', display: 'block', marginBottom: 8 }}>CIBLE (ÉTAT ACTUEL)</Text>
            <Select 
              style={{ width: "100%" }} size="large" value={targetSnapshot} onChange={setTargetSnapshot}
              placeholder="Target version"
            >
              {snapshots.map((s: Snapshot) => <Select.Option key={s.id} value={s.id}>{renderSnapshotLabel(s)}</Select.Option>)}
            </Select>
          </Col>
        </Row>
        <Button 
          type="primary" block size="large" icon={<SearchOutlined />}
          style={{ marginTop: 24, height: 48, fontWeight: "bold" }}
          onClick={handleCompare} loading={loading} disabled={!baseSnapshot || !targetSnapshot}
        >
          LANCER L'AUDIT
        </Button>
      </Card>

      {diffResult ? (
        <Space direction="vertical" size={24} style={{ width: "100%" }}>
          <Card bodyStyle={{ padding: "16px 24px" }}>
            <Row align="middle" gutter={24}>
              <Col span={12}>
                <Space size="large">
                  <Statistic title="CRÉATIONS" value={diffResult.summary.created} valueStyle={{ color: token.colorSuccess }} prefix={<PlusCircleOutlined />} />
                  <Statistic title="MODIFICATIONS" value={diffResult.summary.updated} valueStyle={{ color: token.colorWarning }} prefix={<EditOutlined />} />
                  <Statistic title="SUPPRESSIONS" value={diffResult.summary.deleted} valueStyle={{ color: token.colorError }} prefix={<DeleteOutlined />} />
                </Space>
              </Col>
              <Col span={12} style={{ textAlign: "right" }}>
                <Segmented 
                  options={["all", "contacts", "companies", "deals", "tickets"].map(v => ({ label: v.toUpperCase(), value: v }))}
                  value={objectFilter} onChange={(v) => setObjectFilter(v as string)}
                />
              </Col>
            </Row>
          </Card>

          {filteredData('updated').length > 0 && (
            <Card title={<Space><EditOutlined style={{ color: token.colorWarning }} /> Propriétés Impactées</Space>}>
              <Collapse ghost accordion expandIconPosition="end">
                {filteredData('updated').slice((updatedPage-1)*PAGE_SIZE, updatedPage*PAGE_SIZE).map((item) => (
                  <Panel 
                    header={
                      <Space size="large">
                        <Tag color="blue" style={{ width: 90, textAlign: 'center' }}>{item.type.toUpperCase()}</Tag>
                        <Text strong style={{ fontFamily: 'monospace' }}>ID: {item.id}</Text>
                        <Text type="secondary">{item.display_name}</Text>
                      </Space>
                    } 
                    key={`${item.type}-${item.id}`}
                  >
                    {item.changes && renderChanges(item.changes)}
                  </Panel>
                ))}
              </Collapse>
              <Pagination 
                style={{ marginTop: 16, textAlign: 'right' }}
                current={updatedPage} total={filteredData('updated').length} 
                pageSize={PAGE_SIZE} onChange={setUpdatedPage} size="small"
              />
            </Card>
          )}

          <Row gutter={16}>
            <Col span={12}>
              <Card title={<Space><PlusCircleOutlined style={{ color: token.colorSuccess }} /> Entités Apparues</Space>} bodyStyle={{ maxHeight: 400, overflowY: 'auto' }}>
                {filteredData('created').map(item => (
                  <div key={item.id} style={{ marginBottom: 8, padding: "8px", background: token.colorSuccessBg, border: `1px solid ${token.colorSuccessBorder}`, borderRadius: 6 }}>
                    <Tag color="green">{item.type}</Tag> <Text strong>ID: {item.id}</Text>
                  </div>
                ))}
                {filteredData('created').length === 0 && <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} />}
              </Card>
            </Col>
            <Col span={12}>
              <Card title={<Space><DeleteOutlined style={{ color: token.colorError }} /> Entités Disparues</Space>} bodyStyle={{ maxHeight: 400, overflowY: 'auto' }}>
                {filteredData('deleted').map(item => (
                  <div key={item.id} style={{ marginBottom: 8, padding: "8px", background: token.colorErrorBg, border: `1px solid ${token.colorErrorBorder}`, borderRadius: 6 }}>
                    <Tag color="red">{item.type}</Tag> <Text strong>ID: {item.id}</Text>
                  </div>
                ))}
                {filteredData('deleted').length === 0 && <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} />}
              </Card>
            </Col>
          </Row>

          <Alert
            message="Action de Résilience Recommandée"
            description={`Pour annuler ces ${diffResult.summary.updated} modifications, restaurez SNAP-${String(baseSnapshot).padStart(4, '0')}.`}
            type="warning" showIcon icon={<HistoryOutlined />}
            action={
              <Button type="primary" danger icon={<RollbackOutlined />} onClick={() => router.push(`/snapshots/show/${baseSnapshot}`)}>
                RESTAURER CET ÉTAT
              </Button>
            }
            style={{ padding: "20px", borderRadius: 12 }}
          />
        </Space>
      ) : (
        <Card style={{ textAlign: "center", padding: "80px 0", border: `2px dashed ${token.colorBorderSecondary}`, borderRadius: 16 }}>
          <Empty description="Audit de comparaison en attente" />
        </Card>
      )}
    </div>
  );
}

export default function DiffPage() {
  return (
    <Suspense fallback={<Card><Skeleton active /></Card>}>
      <DiffContent />
    </Suspense>
  );
}