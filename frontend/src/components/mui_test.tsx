import useState from "react";
import Button from "@mui/material/Button";

const MuiTest: React.FC = () => {
  return (
    <>
      <Button color="secondary">Secondary</Button>
      <Button variant="contained" color="success">
        Success
      </Button>
      <Button variant="outlined" color="error">
        Error
      </Button>
    </>
  );
};

export default MuiTest;
