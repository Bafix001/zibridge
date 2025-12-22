import { DevtoolsProvider } from "@providers/devtools";
import { useNotificationProvider } from "@refinedev/antd";
import { Refine } from "@refinedev/core";
import { RefineKbar, RefineKbarProvider } from "@refinedev/kbar";
import routerProvider from "@refinedev/nextjs-router";
import { Metadata } from "next";
import { cookies } from "next/headers";
import React, { Suspense } from "react";

import { AntdRegistry } from "@ant-design/nextjs-registry";
import { ColorModeContextProvider } from "@contexts/color-mode";
import { dataProvider } from "@providers/data-provider";
import { ZibridgeLayout } from "@/components/layout";
import { App } from "antd"; // ✅ Ajouté pour le contexte global (Modals/Messages)
import "@refinedev/antd/dist/reset.css";

export const metadata: Metadata = {
  title: "Zibridge - Git for Data",
  description: "CRM Versioning System with Smart Restore",
  icons: {
    icon: "/favicon.ico",
  },
};

export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const cookieStore = await cookies();
  const theme = cookieStore.get("theme");
  const defaultMode = theme?.value === "dark" ? "dark" : "light";

  return (
    <html lang="fr">
      <body>
        <Suspense>
          <RefineKbarProvider>
            <AntdRegistry>
              <ColorModeContextProvider defaultMode={defaultMode}>
                {/* ✅ App doit entourer Refine et ZibridgeLayout */}
                <App> 
                  <DevtoolsProvider>
                    <Refine
                      routerProvider={routerProvider}
                      dataProvider={dataProvider}
                      notificationProvider={useNotificationProvider}
                      resources={[
                        {
                          name: "snapshots",
                          list: "/snapshots",
                          show: "/snapshots/show/:id",
                          meta: {
                            label: "Snapshots",
                            icon: "📸",
                          },
                        },
                      ]}
                      options={{
                        syncWithLocation: true,
                        warnWhenUnsavedChanges: true,
                        projectId: "KxWnl9-QHAgen-jPvwxr",
                      }}
                    >
                      <ZibridgeLayout>{children}</ZibridgeLayout>
                      <RefineKbar />
                    </Refine>
                  </DevtoolsProvider>
                </App>
              </ColorModeContextProvider>
            </AntdRegistry>
          </RefineKbarProvider>
        </Suspense>
      </body>
    </html>
  );
}