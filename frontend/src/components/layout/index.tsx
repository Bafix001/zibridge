"use client";

import React, { useState, useEffect, useContext } from "react";
import {
  Layout, Menu, Typography, Space, Avatar, Switch, theme,
  Button, Modal, Input, Select, Dropdown, type MenuProps, App
} from "antd";
import {
  SwapOutlined, SettingOutlined, DatabaseOutlined, PlusOutlined,
  EditOutlined, DeleteOutlined, EllipsisOutlined, QuestionCircleOutlined
} from "@ant-design/icons";
import { useRouter, usePathname } from "next/navigation";
import { ColorModeContext } from "@contexts/color-mode";
import { useApiUrl } from "@refinedev/core";

const { Sider, Header, Content } = Layout;
const { Title, Text } = Typography;

interface Project {
  id: number;
  name: string;
  icon: string;
  default_source_type: string;
  config: Record<string, any>;
}

export const ZibridgeLayout: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { modal, message } = App.useApp(); // ✅ Utilisation des hooks (plus de warnings statiques)
  const [collapsed, setCollapsed] = useState(false);
  const router = useRouter();
  const pathname = usePathname();
  const apiUrl = useApiUrl();
  const { mode, setMode } = useContext(ColorModeContext);
  const { token } = theme.useToken();

  const [projects, setProjects] = useState<Project[]>([]);
  const [activeProjectId, setActiveProjectId] = useState<number | null>(null);
  const [newProjectModalOpen, setNewProjectModalOpen] = useState(false);
  const [newProjectName, setNewProjectName] = useState("");
  const [newProjectProvider, setNewProjectProvider] = useState<string>("csv");

  useEffect(() => {
    fetchProjects();
  }, [apiUrl]);

  useEffect(() => {
    const segments = pathname.split("/");
    const projectIndex = segments.indexOf("projects");
    if (projectIndex !== -1 && segments[projectIndex + 1]) {
      setActiveProjectId(parseInt(segments[projectIndex + 1]));
    } else {
      setActiveProjectId(null);
    }
  }, [pathname]);

  const fetchProjects = async () => {
    try {
      const res = await fetch(`${apiUrl}/projects`);
      if (res.ok) {
        const data = await res.json();
        setProjects(Array.isArray(data) ? data : data.data || []);
      }
    } catch (error) {
      console.error("Erreur chargement projets:", error);
    }
  };

  const handleMenuAction = (menuKey: string, project: Project) => {
    if (menuKey === "rename") {
      let inputValue = project.name;
      modal.confirm({
        title: "Renommer le projet",
        content: (
          <Input
            defaultValue={project.name}
            onChange={(e) => (inputValue = e.target.value)}
            style={{ marginTop: 16 }}
            autoFocus
          />
        ),
        okText: "Renommer",
        onOk: async () => {
          if (!inputValue.trim()) return;
          const res = await fetch(`${apiUrl}/projects/${project.id}`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: inputValue.trim() })
          });
          if (res.ok) {
            message.success("✅ Projet renommé");
            fetchProjects();
          }
        }
      });
    }
    
    if (menuKey === "delete") {
      modal.confirm({
        title: "Supprimer ce projet ?",
        content: `"${project.name}" et ses snapshots seront supprimés.`,
        okText: "Supprimer",
        okButtonProps: { danger: true },
        onOk: async () => {
          const res = await fetch(`${apiUrl}/projects/${project.id}`, { method: "DELETE" });
          if (res.ok) {
            message.success("🗑️ Projet supprimé");
            fetchProjects();
            if (activeProjectId === project.id) router.push("/");
          }
        }
      });
    }
  };

  const handleCreateProject = async () => {
    if (!newProjectName.trim()) {
      message.error("Le nom est obligatoire");
      return;
    }
    try {
      const iconMap: any = { csv: "📁", hubspot: "🟠", salesforce: "☁️", pipedrive: "🟢", other: "📸" };
      const res = await fetch(`${apiUrl}/projects`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: newProjectName,
          icon: iconMap[newProjectProvider] || "📸",
          default_source_type: newProjectProvider === "csv" ? "file" : "api",
          config: { provider: newProjectProvider },
        }),
      });
      if (res.ok) {
        const newProj = await res.json();
        message.success("✅ Projet créé");
        setNewProjectModalOpen(false);
        setNewProjectName("");
        await fetchProjects();
        router.push(`/projects/${newProj.id}`);
      }
    } catch (e) { message.error("Erreur serveur"); }
  };

  return (
    <Layout style={{ minHeight: "100vh" }}>
      <Sider
        collapsible collapsed={collapsed} onCollapse={setCollapsed}
        style={{
          overflow: "auto", height: "100vh", position: "fixed", left: 0,
          background: token.colorBgContainer, borderRight: `1px solid ${token.colorBorderSecondary}`, zIndex: 10,
        }}
        theme={mode === "dark" ? "dark" : "light"}
        width={250}
      >
        {/* LOGO SECTION */}
        <div style={{ height: 64, display: "flex", alignItems: "center", justifyContent: collapsed ? "center" : "flex-start", padding: collapsed ? 0 : "0 24px", borderBottom: `1px solid ${token.colorBorderSecondary}` }}>
          <Space size="small">
            <DatabaseOutlined style={{ fontSize: 24, color: token.colorPrimary }} />
            {!collapsed && (
              <Title level={4} style={{ margin: 0, background: `linear-gradient(135deg, ${token.colorPrimary} 0%, ${token.colorPrimaryHover} 100%)`, WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent" }}>
                Zibridge
              </Title>
            )}
          </Space>
        </div>

        {/* PROJECTS SECTION */}
        {!collapsed && (
          <div style={{ padding: "16px" }}>
            <Space style={{ width: "100%", justifyContent: "space-between", marginBottom: 12 }}>
              <Text type="secondary" strong style={{ fontSize: 10, letterSpacing: "0.5px" }}>VOS PROJETS</Text>
              <Button type="text" size="small" icon={<PlusOutlined />} onClick={() => setNewProjectModalOpen(true)} style={{ color: token.colorPrimary }} />
            </Space>

            <div style={{ maxHeight: "calc(100vh - 350px)", overflowY: "auto" }}>
              {projects.length === 0 ? (
                <Text type="secondary" style={{ fontSize: 12, display: "block", textAlign: "center", padding: "8px" }}>Aucun projet</Text>
              ) : (
                <Space direction="vertical" style={{ width: "100%" }} size={4}>
                  {projects.map((project) => (
                    <div 
                      key={project.id}
                      onMouseEnter={(e) => (e.currentTarget.querySelector(".menu-dots") as any).style.opacity = "1"}
                      onMouseLeave={(e) => (e.currentTarget.querySelector(".menu-dots") as any).style.opacity = "0"}
                      style={{ 
                        display: "flex", alignItems: "center", borderRadius: 8, paddingRight: 4,
                        background: activeProjectId === project.id ? token.colorPrimaryBg : "transparent"
                      }}
                    >
                      <Button
                        type="text" block
                        onClick={() => router.push(`/projects/${project.id}`)}
                        style={{ textAlign: "left", flex: 1, display: "flex", alignItems: "center", height: 36, fontWeight: activeProjectId === project.id ? 600 : 400 }}
                      >
                        <span style={{ marginRight: 8 }}>{project.icon}</span>
                        <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{project.name}</span>
                      </Button>

                      <Dropdown
                        menu={{ 
                          items: [
                            { key: "rename", label: "Renommer", icon: <EditOutlined /> },
                            { type: "divider" },
                            { key: "delete", label: "Supprimer", icon: <DeleteOutlined />, danger: true }
                          ],
                          onClick: ({ key, domEvent }) => {
                            domEvent.stopPropagation();
                            handleMenuAction(key, project);
                          }
                        }}
                        trigger={["click"]}
                      >
                        <Button type="text" size="small" icon={<EllipsisOutlined />} className="menu-dots" style={{ opacity: 0, transition: "0.2s" }} onClick={(e) => e.stopPropagation()} />
                      </Dropdown>
                    </div>
                  ))}
                </Space>
              )}
            </div>
          </div>
        )}

        {/* MAIN MENU */}
        <Menu
          theme={mode === "dark" ? "dark" : "light"}
          mode="inline"
          selectedKeys={[pathname]}
          items={[
            { key: "/diff", icon: <SwapOutlined />, label: "Comparer" },
            { key: "/settings", icon: <SettingOutlined />, label: "Paramètres" },
          ]}
          onClick={({ key }) => router.push(key)}
          style={{ borderRight: 0, marginTop: 8 }}
        />

        {/* USER FOOTER */}
        {!collapsed && (
          <div style={{ position: "absolute", bottom: 0, width: "100%", padding: "16px", borderTop: `1px solid ${token.colorBorderSecondary}`, background: token.colorBgContainer }}>
            <Space>
              <Avatar style={{ backgroundColor: token.colorPrimary }}>A</Avatar>
              <div style={{ lineHeight: 1 }}>
                <Text strong style={{ display: "block", fontSize: 12 }}>Admin</Text>
                <Text type="secondary" style={{ fontSize: 10 }}>v1.3</Text>
              </div>
            </Space>
          </div>
        )}
      </Sider>

      {/* MAIN CONTENT AREA */}
      <Layout style={{ marginLeft: collapsed ? 80 : 250, transition: "margin-left 0.2s" }}>
        <Header style={{ padding: "0 24px", background: token.colorBgContainer, display: "flex", justifyContent: "flex-end", alignItems: "center", borderBottom: `1px solid ${token.colorBorderSecondary}`, position: "sticky", top: 0, zIndex: 9 }}>
          <Switch checkedChildren="🌛" unCheckedChildren="🔆" checked={mode === "dark"} onChange={() => setMode(mode === "light" ? "dark" : "light")} />
        </Header>
        <Content style={{ margin: "24px" }}>
          <div style={{ background: token.colorBgContainer, padding: 24, borderRadius: 12, minHeight: "100%" }}>
            {children}
          </div>
        </Content>
      </Layout>

      {/* NEW PROJECT MODAL */}
      <Modal
        title={<b>📁 Nouveau projet</b>}
        open={newProjectModalOpen}
        onOk={handleCreateProject}
        onCancel={() => setNewProjectModalOpen(false)}
        okText="Créer"
        centered
      >
        <Space direction="vertical" style={{ width: "100%", marginTop: 16 }} size="large">
          <div>
            <Text strong>Nom du projet</Text>
            <Input size="large" placeholder="Ex: Clients Q4" value={newProjectName} onChange={(e) => setNewProjectName(e.target.value)} style={{ marginTop: 8 }} />
          </div>
          <div>
            <Text strong>Source de données</Text>
            <Select
              size="large" value={newProjectProvider} onChange={setNewProjectProvider} style={{ width: "100%", marginTop: 8 }}
              options={[
                { label: "📁 Fichier CSV", value: "csv" },
                { label: "🟠 HubSpot", value: "hubspot" },
                { label: "☁️ Salesforce", value: "salesforce" },
                { label: "📸 Autres", value: "other" },
              ]}
            />
          </div>
        </Space>
      </Modal>
    </Layout>
  );
};