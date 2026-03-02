import express from "express";
import { PrismaClient } from "@prisma/client";

const app = express();
const prisma = new PrismaClient();

app.use(express.json());

app.post("/identify", async (req, res) => {
  try {
    const { email, phoneNumber } = req.body;

    if (!email && !phoneNumber) {
      return res.status(400).json({
        error: "Email or phoneNumber required"
      });
    }

    // STEP 1: Find all contacts matching email or phone
    const matchedContacts = await prisma.contact.findMany({
      where: {
        OR: [
          { email: email ?? undefined },
          { phoneNumber: phoneNumber ?? undefined }
        ]
      },
      orderBy: {
        createdAt: "asc"
      }
    });

    // CASE 1: No matches → create new primary
    if (matchedContacts.length === 0) {
      const newContact = await prisma.contact.create({
        data: {
          email,
          phoneNumber,
          linkPrecedence: "primary"
        }
      });

      return res.status(200).json({
        contact: {
          primaryContactId: newContact.id,
          emails: newContact.email ? [newContact.email] : [],
          phoneNumbers: newContact.phoneNumber ? [newContact.phoneNumber] : [],
          secondaryContactIds: []
        }
      });
    }

    // STEP 2: Get all primary contacts among matches
    const primaryContacts = matchedContacts.filter(
      c => c.linkPrecedence === "primary"
    );

    // Sort by createdAt → oldest first
    primaryContacts.sort(
      (a, b) =>
        new Date(a.createdAt).getTime() -
        new Date(b.createdAt).getTime()
    );

    const oldestPrimary = primaryContacts[0];

    // STEP 3: If more than one primary → merge them
    if (primaryContacts.length > 1) {
      for (let i = 1; i < primaryContacts.length; i++) {
        const newerPrimary = primaryContacts[i];

        // Convert newer primary → secondary
        await prisma.contact.update({
          where: { id: newerPrimary.id },
          data: {
            linkPrecedence: "secondary",
            linkedId: oldestPrimary.id
          }
        });

        // Update all its children to point to oldestPrimary
        await prisma.contact.updateMany({
          where: { linkedId: newerPrimary.id },
          data: { linkedId: oldestPrimary.id }
        });
      }
    }

    // STEP 4: Get full cluster
    const clusterContacts = await prisma.contact.findMany({
      where: {
        OR: [
          { id: oldestPrimary.id },
          { linkedId: oldestPrimary.id }
        ]
      },
      orderBy: { createdAt: "asc" }
    });

    // STEP 5: Check if new info needs secondary creation
    const emailExists = clusterContacts.some(c => c.email === email);
    const phoneExists = clusterContacts.some(c => c.phoneNumber === phoneNumber);

    if (!emailExists || !phoneExists) {
      await prisma.contact.create({
        data: {
          email,
          phoneNumber,
          linkedId: oldestPrimary.id,
          linkPrecedence: "secondary"
        }
      });
    }

    // STEP 6: Fetch updated cluster
    const finalContacts = await prisma.contact.findMany({
      where: {
        OR: [
          { id: oldestPrimary.id },
          { linkedId: oldestPrimary.id }
        ]
      },
      orderBy: { createdAt: "asc" }
    });

    const emails = [
      ...new Set(finalContacts.map(c => c.email).filter(Boolean))
    ];

    const phoneNumbers = [
      ...new Set(finalContacts.map(c => c.phoneNumber).filter(Boolean))
    ];

    const secondaryContactIds = finalContacts
      .filter(c => c.linkPrecedence === "secondary")
      .map(c => c.id);

    return res.status(200).json({
      contact: {
        primaryContactId: oldestPrimary.id,
        emails,
        phoneNumbers,
        secondaryContactIds
      }
    });

  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Internal server error" });
  }
});

app.listen(3000, () => {
  console.log("Server running on port 3000");
});