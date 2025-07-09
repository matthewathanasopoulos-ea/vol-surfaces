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
        <p />
        {showCMEReport && Object.keys(ctaFlowToTradeResponse).length > 0 && (
          <div>
            <TextArea
              rows={10}
              style={{
                width: "100%",
                paddingRight: "10px",
                paddingBottom: "10px",
                fontFamily: "inherit",
              }}
              defaultValue={`Gold: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.gold}k lots over the coming week.

Silver: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.silver}k lots over the coming week.

Platinum: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.platinum}k lots over the coming week.

Copper: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.copper}k lots over the coming week.

Palladium: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.palladium}k lots over the coming week.`}
            />
            <iframe
              title="Weekly Position Changes, CME"
              aria-label="Table"
              id="datawrapper-chart-Ppxlt"
              src="https://datawrapper.dwcdn.net/Ppxlt/1767/"
              scrolling="no"
              frameBorder="0"
              style={{ paddingTop: "20px" }}
              width="664"
              height="276"
              data-external="1"
            ></iframe>
            <iframe
              title="CME metal position changes, k lots (%)"
              aria-label="Table"
              id="datawrapper-chart-SNBsu"
              src="https://datawrapper.dwcdn.net/SNBsu/6252/"
              scrolling="no"
              frameBorder="0"
              style={{ paddingTop: "20px" }}
              width="771"
              height="430"
              data-external="1"
            ></iframe>
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
        <p />
        {showLMEReport && Object.keys(ctaFlowToTradeResponse).length > 0 && (
          <div>
            <TextArea
              rows={10}
              style={{ width: "100%", paddingRight: "10px", fontFamily: "inherit" }}
              defaultValue={`Aluminium: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.lme_aluminium}k lots over the coming week.

Copper: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.lme_copper}k lots over the coming week.

Lead: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.lme_lead}k lots over the coming week.

Nickel: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.lme_nickel}k lots over the coming week.

Zinc: CTAs bought 0.2k lots, and discretionary funds sold 0.7k lots. CTAs are expected to ${ctaFlowToTradeResponse.lme_zinc}k lots over the coming week.`}
            />
            <iframe
              title="Weekly Position Changes, LME"
              aria-label="Table"
              id="datawrapper-chart-tKRyh"
              src="https://datawrapper.dwcdn.net/tKRyh/1369/"
              scrolling="no"
              frameBorder="0"
              style={{ paddingTop: "20px" }}
              width="600"
              height="276"
              data-external="1"
            ></iframe>
            <iframe
              title=""
              aria-label="Table"
              id="datawrapper-chart-oS3fW"
              src="https://datawrapper.dwcdn.net/oS3fW/6244/"
              scrolling="no"
              frameBorder="0"
              style={{ paddingTop: "20px" }}
              width="750"
              height="335"
              data-external="1"
            ></iframe>
          </div>
        )}
      </div>
    </div>
  );
};

export default MetalsPositionUpdates;
