// Smart pickup / call opening lines for Mc Scout.
// Categories: friday, monday, morning, afternoon, evening, weekend,
// reefer, fresh, general, emergency, followup, first_contact

const PICKUP_LINES = {
  friday: [
    "Hey, happy Friday! Just checking if you're heading back home this weekend or if you'd be available for any loads before you go?",
    "Happy Friday! Got a couple of loads that could work before the weekend — you interested or heading home?",
    "Hey, hope your week's wrapping up well. Any chance you're free for one more run before the weekend?",
  ],
  monday: [
    "Hey, hope you had a good weekend! Starting the week off — I've got some freight that might fit your route, you open?",
    "Morning! Fresh week, fresh loads. Are you back on the road today or still heading out?",
  ],
  morning: [
    "Good morning! Hope you're doing well. I've got some freight opportunities that may work with your current location. Are you looking for anything for today or tomorrow?",
    "Morning! Quick one — are you empty or loaded right now? Got something that might line up.",
  ],
  afternoon: [
    "Hey, hope your day's going smooth. I've got a load that might fit your afternoon — you free to talk?",
    "Afternoon! Checking in — any chance you're looking for your next load already?",
  ],
  evening: [
    "Hey, hope you had a solid day out there. Got something lined up for tomorrow morning if you're interested?",
    "Evening! Wrapping up for the day or still rolling? Got a load that might work either way.",
  ],
  weekend: [
    "Hey, hope you're enjoying the weekend. If you're around and open to a load, I've got something that might work.",
    "Hope you're getting some rest this weekend! If anything urgent comes up I'll keep you in mind.",
  ],
  reefer: [
    "Hey, I've got a reefer load that needs to move — temp-controlled, good rate. You available?",
    "Got a cold-chain load lined up, thought of you first. Interested in the details?",
  ],
  fresh: [
    "Hey, I've got a fresh produce load that needs quick turnaround — you free to run it?",
    "Time-sensitive fresh load just came in. You in a position to grab it?",
  ],
  general: [
    "Got a general freight load that might fit your lane — want the details?",
    "Hey, dry van load available on your usual route. You interested?",
  ],
  emergency: [
    "Hey, sorry for the short notice — I've got an urgent load that needs to move ASAP. Any chance you can help out?",
    "Emergency load just dropped, needs someone reliable fast. Can you take it on short notice?",
  ],
  followup: [
    "Hey, just following up from earlier — did that load end up working for you?",
    "Circling back on our last conversation — still interested in loads on that lane?",
  ],
  first_contact: [
    "Hi, this is Mc Scout dispatch reaching out — we work with owner-operators on regular freight. Mind if I share a bit about what we've got?",
    "Hey, first time reaching out — we've got consistent freight and wanted to see if it's a fit for your operation.",
  ],
};

function getSmartPickupLine(categoryOverride) {
  let category = categoryOverride;
  if (!category) {
    const now = new Date();
    const day = now.getDay(); // 0 Sun - 6 Sat
    const hour = now.getHours();
    if (day === 5) category = 'friday';
    else if (day === 1) category = 'monday';
    else if (day === 0 || day === 6) category = 'weekend';
    else if (hour < 12) category = 'morning';
    else if (hour < 17) category = 'afternoon';
    else category = 'evening';
  }
  const pool = PICKUP_LINES[category] || PICKUP_LINES.first_contact;
  return pool[Math.floor(Math.random() * pool.length)];
}
