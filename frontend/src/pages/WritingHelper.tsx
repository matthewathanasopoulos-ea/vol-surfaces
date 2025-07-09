import React, { useState } from "react";
import MetalsPositionUpdates from "../components/MetalsPositionUpdates";
import { Button, Breadcrumb, Drawer } from "antd";

const WritingHelper = () => {
  const [openDrawer, setOpenDrawer] = useState(false);
  const [error, setError] = useState<Error>();

  if (error) {
    return <p>{error.message}</p>;
  }

  return (
    <>
      <div style={{ padding: "20px" }}>
        <div style={{ display: "flex", gap: "20px" }}>
          <Breadcrumb
            items={[
              {
                title: <a href="/">Home</a>,
              },
              {
                title: "Writing Helper",
              },
            ]}
          />
          <Button color="danger" variant="solid" onClick={() => setOpenDrawer(true)}>
            LLM Parameter Tuning Options
          </Button>
        </div>
        <h2>Matthew Writing Helpers</h2>
        <MetalsPositionUpdates />
      </div>

      <Drawer
        title="LLM Tuning Parameters"
        closable={{ "aria-label": "Close Button" }}
        onClose={() => setOpenDrawer(false)}
        open={openDrawer}
      >
        <p>Temperature</p>
        <p>Top P</p>
        <p>Frequency Penalty</p>
      </Drawer>
    </>
  );
};

export default WritingHelper;
