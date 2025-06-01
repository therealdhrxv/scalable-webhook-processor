import cors from "cors";
import express from "express";

import { zapRouter } from "./router/zap";
import { userRouter } from "./router/user";

const app = express();

app.use(express.json());
app.use(cors());

app.use("/api/v1/user", userRouter);
app.use("/api/v1/zap", zapRouter);

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
	console.log(`Primary Backend is running on port ${PORT}`);
});
