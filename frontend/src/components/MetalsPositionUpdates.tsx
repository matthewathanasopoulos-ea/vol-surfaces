import React, { useState, useEffect } from "react";
import { Button, Input } from "antd";

const { TextArea } = Input;

const MetalsPositionUpdates = () => {
  const [showCMEReport, setShowCMEReport] = useState<boolean>(false);
  const [showLMEReport, setShowLMEReport] = useState<boolean>(false);
  const [ctaFlowToTradeResponse, setCtaFlowToTradeResponse] = useState<Record<string, number>>({});
  const [isDisabled, setIsDisabled] = useState<boolean>(true);
  const [error, setError] = useState<string>("");

  const handleCTAFlowToTrade = async (): Promise<void> => {
    setError("");

    try {
      const response = await fetch("http://127.0.0.1:5000/cta-flow-to-trade");

      if (!response.ok) {
        setError(`HTTP error! status: ${response.status}`);
      }

      const data: Record<string, number> = await response.json();

      setCtaFlowToTradeResponse(data);
      setIsDisabled(false);
    } catch (err) {
      setIsDisabled(true);
    }
  };

  useEffect(() => {
    handleCTAFlowToTrade();
  }, []);

  return (
    <div style={{ display: "flex" }}>
      <div style={{ width: "45%", paddingRight: "50px" }}>
        <Button
          color="cyan"
          variant="solid"
          disabled={isDisabled}
          onClick={() => {
            setShowCMEReport(true);
          }}
        >
          Generate Report for CME Metals Position Update
        </Button>
        {showCMEReport && Object.keys(ctaFlowToTradeResponse).length > 0 && (
          <div>
            <h3>CME Metals Position Update:</h3>
            <TextArea
              rows={16}
              style={{ width: "100%", paddingRight: "10px", fontFamily: "inherit" }}
              defaultValue={`Gold: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.gold}k lots over the coming week.

Silver: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.silver}k lots over the coming week.

Platinum: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.platinum}k lots over the coming week.

Copper: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.copper}k lots over the coming week.

Palladium: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.palladium}k lots over the coming week.`}
            />
          </div>
        )}
      </div>
      <div style={{ width: "50%" }}>
        <Button
          color="cyan"
          variant="solid"
          disabled={isDisabled}
          onClick={() => {
            setShowLMEReport(true);
          }}
        >
          Generate Report for LME Metals Position Update
        </Button>
        {showLMEReport && Object.keys(ctaFlowToTradeResponse).length > 0 && (
          <div>
            <h3>LME Metals Position Update:</h3>
            <TextArea
              rows={16}
              style={{ width: "100%", paddingRight: "10px", fontFamily: "inherit" }}
              defaultValue={`Aluminium: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.lme_aluminium}k lots over the coming week.

Copper: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.lme_copper}k lots over the coming week.

Lead: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.lme_lead}k lots over the coming week.

Nickel: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.lme_nickel}k lots over the coming week.

Zinc: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.lme_zinc}k lots over the coming week.`}
            />
          </div>
        )}
      </div>
    </div>
  );
};

export default MetalsPositionUpdates;
