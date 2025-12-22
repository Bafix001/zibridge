"use client";

import { ColorModeContext } from "@contexts/color-mode";
import type { RefineThemedLayoutHeaderProps } from "@refinedev/antd";
import { useGetIdentity } from "@refinedev/core";
import {
  Layout as AntdLayout,
  Avatar,
  Space,
  Switch,
  theme,
  Typography,
  Badge,
  Tooltip,
} from "antd";
import React, { useContext } from "react";
import { 
  ApiOutlined, 
  MoonFilled, 
  SunFilled 
} from "@ant-design/icons";

const { Text } = Typography;
const { useToken } = theme;

type IUser = {
  id: number;
  name: string;
  avatar: string;
};

export const Header: React.FC<RefineThemedLayoutHeaderProps> = ({
  sticky = true,
}) => {
  const { token } = useToken();
  const { data: user } = useGetIdentity<IUser>();
  const { mode, setMode } = useContext(ColorModeContext);

  // Styles adaptatifs pour le Dark/Light mode
  const headerStyles: React.CSSProperties = {
    backgroundColor: token.colorBgElevated,
    display: "flex",
    justifyContent: "space-between", // ✅ Changé pour écarter le statut et le profil
    alignItems: "center",
    padding: "0px 24px",
    height: "64px",
    borderBottom: `1px solid ${token.colorBorderSecondary}`, // ✅ Ligne de séparation subtile
    boxShadow: mode === "light" ? "0 2px 8px rgba(0,0,0,0.05)" : "none",
  };

  if (sticky) {
    headerStyles.position = "sticky";
    headerStyles.top = 0;
    headerStyles.zIndex = 10;
  }

  return (
    <AntdLayout.Header style={headerStyles}>
      {/* ✅ Côté Gauche : Statut du Connecteur (Agnostique) */}
      <Space>
        <Tooltip title="Statut de la connexion CRM">
          <Badge status="processing" color={token.colorPrimary} />
          <Text type="secondary" style={{ fontSize: "12px", fontFamily: "monospace" }}>
            <ApiOutlined /> ENGINE_ACTIVE
          </Text>
        </Tooltip>
      </Space>

      {/* ✅ Côté Droit : Theme Switcher & User Profil */}
      <Space size="large">
        <Switch
          checkedChildren={<MoonFilled style={{ fontSize: "12px" }} />}
          unCheckedChildren={<SunFilled style={{ fontSize: "12px" }} />}
          onChange={() => setMode(mode === "light" ? "dark" : "light")}
          checked={mode === "dark"}
          style={{ backgroundColor: mode === "dark" ? token.colorPrimary : "" }}
        />
        
        {(user?.name || user?.avatar) && (
          <Space size="middle" style={{ borderLeft: `1px solid ${token.colorBorder}`, paddingLeft: "16px" }}>
            <div style={{ textAlign: "right", lineHeight: "1.2" }}>
              <Text strong style={{ display: "block", fontSize: "13px" }}>
                {user.name}
              </Text>
              <Text type="secondary" style={{ fontSize: "11px" }}>
                Administrator
              </Text>
            </div>
            <Avatar 
              src={user?.avatar} 
              alt={user?.name} 
              style={{ backgroundColor: token.colorPrimary, verticalAlign: 'middle' }}
              size="large"
            >
              {user?.name?.[0]}
            </Avatar>
          </Space>
        )}
      </Space>
    </AntdLayout.Header>
  );
};